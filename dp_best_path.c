#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;


typedef struct {
    int32 A, B, C, D, E;
    int64 sumW; 
} PathState;

typedef struct {
    int32 key; 
    int n_paths;
    int k_limit;
    PathState *paths;
} NodeEntry;


static void swap_paths(PathState *a, PathState *b)
{
    PathState tmp = *a;
    *a = *b;
    *b = tmp;
}

static void heapify_down(PathState heap[], int n, int i)
{
    int smallest = i;
    int l = 2*i + 1;
    int r = 2*i + 2;

    if (l < n && heap[l].sumW < heap[smallest].sumW)
        smallest = l;
    if (r < n && heap[r].sumW < heap[smallest].sumW)
        smallest = r;

    if (smallest != i)
    {
        swap_paths(&heap[i], &heap[smallest]);
        heapify_down(heap, n, smallest);
    }
}

static void heapify_up(PathState heap[], int i)
{
    int parent = (i - 1) / 2;
    while (i > 0 && heap[i].sumW < heap[parent].sumW)
    {
        swap_paths(&heap[i], &heap[parent]);
        i = parent;
        parent = (i - 1) / 2;
    }
}

static void insert_path_topk(NodeEntry *entry, PathState *new_path, int k)
{
    if (entry->paths == NULL)
    {
        entry->paths = palloc(sizeof(PathState) * k);
        entry->n_paths = 0;
        entry->k_limit = k;
    }

    if (entry->n_paths < k)
    {
        entry->paths[entry->n_paths] = *new_path;
        heapify_up(entry->paths, entry->n_paths);
        entry->n_paths++;
    }
    else
    {
        if (new_path->sumW > entry->paths[0].sumW)
        {
            entry->paths[0] = *new_path;
            heapify_down(entry->paths, k, 0);
        }
    }
}


static HTAB *create_layer(const char *name, MemoryContext ctx)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int32);
    ctl.entrysize = sizeof(NodeEntry);
    ctl.hcxt = ctx;
    return hash_create(name, 10000, &ctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
}

PG_FUNCTION_INFO_V1(dp_best_path);

Datum dp_best_path(PG_FUNCTION_ARGS)
{
    text *r_table = PG_GETARG_TEXT_PP(0);
    text *s_table = PG_GETARG_TEXT_PP(1);
    text *t_table = PG_GETARG_TEXT_PP(2);
    text *u_table = PG_GETARG_TEXT_PP(3);
    text *r_join_col = PG_GETARG_TEXT_PP(4); 
    text *s_join_col = PG_GETARG_TEXT_PP(5); 
    text *t_join_col = PG_GETARG_TEXT_PP(6); 
    int32 k = PG_GETARG_INT32(7);            
    
    if (k <= 0)
        ereport(ERROR, (errmsg("K positive")));

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    if (!rsinfo || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errmsg("set-valued function called in context that cannot accept a set")));

    rsinfo->returnMode = SFRM_Materialize;

    MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    MemoryContext oldcontext;

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("SPI_connect failed")));

    oldcontext = MemoryContextSwitchTo(per_query_ctx);


    rsinfo->setResult = tuplestore_begin_heap(false, false, 0);
    
    TupleDesc tupdesc;
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    {
        tupdesc = CreateTemplateTupleDesc(6);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "a", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "b", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "c", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "d", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "e", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "sumw", INT4OID, -1, 0);
    }
    tupdesc = BlessTupleDesc(tupdesc);

    char sql[512];
    char *r_join = TextDatumGetCString(PointerGetDatum(r_join_col));
    char *s_join = TextDatumGetCString(PointerGetDatum(s_join_col));
    char *t_join = TextDatumGetCString(PointerGetDatum(t_join_col));
    
    /* ========== LAYER 0: R nodes (keyed by A, contains W1) ========== */
    HTAB *layer_R = create_layer("LayerR", per_query_ctx);
    
    snprintf(sql, sizeof(sql), "SELECT A, %s, W1 FROM %s", r_join,
             TextDatumGetCString(PointerGetDatum(r_table)));
    int ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("SPI_execute failed for R table")));
    
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        bool isnull;
        int32 A = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull) continue;
        int32 B = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
        if (isnull) continue;
        int32 W1 = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
        if (isnull) continue;
        
        PathState path;
        memset(&path, 0, sizeof(PathState));
        path.A = A;
        path.B = B;
        path.sumW = (int64)W1;
        
        bool found;
        NodeEntry *entry = (NodeEntry *) hash_search(layer_R, &A, HASH_ENTER, &found);
        if (!found)
        {
            entry->n_paths = 0;
            entry->paths = NULL;
        }
        
        insert_path_topk(entry, &path, k);
    }
    
    /* ========== LAYER 1: RS-join nodes (keyed by B) ========== */
    HTAB *layer_RS = create_layer("LayerRS", per_query_ctx);
    
    HASH_SEQ_STATUS scan;
    NodeEntry *r_entry;
    hash_seq_init(&scan, layer_R);
    while ((r_entry = (NodeEntry *) hash_seq_search(&scan)) != NULL)
    {
        for (int i = 0; i < r_entry->n_paths; i++)
        {
            PathState *path = &r_entry->paths[i];
            int32 B = path->B;
            
            bool found;
            NodeEntry *b_entry = (NodeEntry *) hash_search(layer_RS, &B, HASH_ENTER, &found);
            if (!found)
            {
                b_entry->n_paths = 0;
                b_entry->paths = NULL;
            }
            
            insert_path_topk(b_entry, path, k);
        }
    }
    
    /* ========== LAYER 2: S nodes (keyed by B, contains W2) ========== */
    HTAB *layer_S = create_layer("LayerS", per_query_ctx);
    
    snprintf(sql, sizeof(sql), "SELECT %s, %s, W2 FROM %s", r_join, s_join,
             TextDatumGetCString(PointerGetDatum(s_table)));
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("SPI_execute failed for S table")));
    
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        bool isnull;
        int32 B = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull) continue;
        int32 C = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
        if (isnull) continue;
        int32 W2 = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
        if (isnull) continue;
        
        /* Find paths at RS-join node B */
        bool found;
        NodeEntry *rs_entry = (NodeEntry *) hash_search(layer_RS, &B, HASH_FIND, &found);
        if (!found) continue;
        
        /* Propagate each path from RS-join to S, adding W2 */
        for (int j = 0; j < rs_entry->n_paths; j++)
        {
            PathState new_path = rs_entry->paths[j];
            new_path.C = C;
            new_path.sumW += W2;
            
            /* Key by B (S node identifier) */
            NodeEntry *s_entry = (NodeEntry *) hash_search(layer_S, &B, HASH_ENTER, &found);
            if (!found)
            {
                s_entry->n_paths = 0;
                s_entry->paths = NULL;
            }
            
            insert_path_topk(s_entry, &new_path, k);
        }
    }
    
    /* Free layer_RS */
    hash_destroy(layer_RS);
    
    /* ========== LAYER 3: ST-join nodes (keyed by C) ========== */
    /* Propagate from S nodes to C nodes using S.C values */
    HTAB *layer_ST = create_layer("LayerST", per_query_ctx);
    
    NodeEntry *s_entry;
    hash_seq_init(&scan, layer_S);
    while ((s_entry = (NodeEntry *) hash_seq_search(&scan)) != NULL)
    {
        for (int i = 0; i < s_entry->n_paths; i++)
        {
            PathState *path = &s_entry->paths[i];
            int32 C = path->C;
            
            bool found;
            NodeEntry *c_entry = (NodeEntry *) hash_search(layer_ST, &C, HASH_ENTER, &found);
            if (!found)
            {
                c_entry->n_paths = 0;
                c_entry->paths = NULL;
            }
            
            insert_path_topk(c_entry, path, k);
        }
    }
    
    /* Free layer_S */
    hash_destroy(layer_S);
    
    /* ========== LAYER 4: T nodes (keyed by C, contains W3) ========== */
    /* From ST-join nodes (C), add W3 and propagate to T nodes (with D) */
    HTAB *layer_T = create_layer("LayerT", per_query_ctx);
    
    snprintf(sql, sizeof(sql), "SELECT %s, %s, W3 FROM %s", s_join, t_join,
             TextDatumGetCString(PointerGetDatum(t_table)));
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("SPI_execute failed for T table")));
    
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        bool isnull;
        int32 C = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull) continue;
        int32 D = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
        if (isnull) continue;
        int32 W3 = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
        if (isnull) continue;
        
        /* Find paths at ST-join node C */
        bool found;
        NodeEntry *st_entry = (NodeEntry *) hash_search(layer_ST, &C, HASH_FIND, &found);
        if (!found) continue;
        
        /* Propagate each path from ST-join to T, adding W3 */
        for (int j = 0; j < st_entry->n_paths; j++)
        {
            PathState new_path = st_entry->paths[j];
            new_path.D = D;
            new_path.sumW += W3;
            
            /* Key by C (T node identifier) */
            NodeEntry *t_entry = (NodeEntry *) hash_search(layer_T, &C, HASH_ENTER, &found);
            if (!found)
            {
                t_entry->n_paths = 0;
                t_entry->paths = NULL;
            }
            
            insert_path_topk(t_entry, &new_path, k);
        }
    }
    
    /* Free layer_ST */
    hash_destroy(layer_ST);
    
    /* ========== LAYER 5: TU-join nodes (keyed by D) ========== */
    /* Propagate from T nodes to D nodes using T.D values */
    HTAB *layer_TU = create_layer("LayerTU", per_query_ctx);
    
    NodeEntry *t_entry;
    hash_seq_init(&scan, layer_T);
    while ((t_entry = (NodeEntry *) hash_seq_search(&scan)) != NULL)
    {
        for (int i = 0; i < t_entry->n_paths; i++)
        {
            PathState *path = &t_entry->paths[i];
            int32 D = path->D;
            
            bool found;
            NodeEntry *d_entry = (NodeEntry *) hash_search(layer_TU, &D, HASH_ENTER, &found);
            if (!found)
            {
                d_entry->n_paths = 0;
                d_entry->paths = NULL;
            }
            
            insert_path_topk(d_entry, path, k);
        }
    }
    
    /* Free layer_T */
    hash_destroy(layer_T);
    
    /* ========== LAYER 6: U nodes (keyed by D, contains W4) ========== */
    /* From TU-join nodes (D), add W4 and propagate to U nodes (with E) */
    HTAB *layer_U = create_layer("LayerU", per_query_ctx);
    
    snprintf(sql, sizeof(sql), "SELECT %s, E, W4 FROM %s", t_join,
             TextDatumGetCString(PointerGetDatum(u_table)));
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("SPI_execute failed for U table")));
    
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        bool isnull;
        int32 D = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull) continue;
        int32 E = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
        if (isnull) continue;
        int32 W4 = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
        if (isnull) continue;
        
        bool found;
        NodeEntry *tu_entry = (NodeEntry *) hash_search(layer_TU, &D, HASH_FIND, &found);
        if (!found) continue;
        
        for (int j = 0; j < tu_entry->n_paths; j++)
        {
            PathState new_path = tu_entry->paths[j];
            new_path.E = E;
            new_path.sumW += W4;
            
            NodeEntry *u_entry = (NodeEntry *) hash_search(layer_U, &D, HASH_ENTER, &found);
            if (!found)
            {
                u_entry->n_paths = 0;
                u_entry->paths = NULL;
            }
            
            insert_path_topk(u_entry, &new_path, k);
        }
    }
    
    hash_destroy(layer_TU);
    
    /* ========== Collect all paths and get global top-K ========== */
    PathState *global_topk = palloc(sizeof(PathState) * k);
    int global_count = 0;
    
    NodeEntry *u_entry;
    hash_seq_init(&scan, layer_U);
    while ((u_entry = (NodeEntry *) hash_seq_search(&scan)) != NULL)
    {
        for (int i = 0; i < u_entry->n_paths; i++)
        {
            PathState *p = &u_entry->paths[i];
            
            if (global_count < k)
            {
                global_topk[global_count] = *p;
                global_count++;
                
                for (int j = global_count - 1; j > 0; j--)
                {
                    if (global_topk[j].sumW > global_topk[j-1].sumW)
                    {
                        PathState temp = global_topk[j];
                        global_topk[j] = global_topk[j-1];
                        global_topk[j-1] = temp;
                    }
                    else
                        break;
                }
            }
            else if (p->sumW > global_topk[k - 1].sumW)
            {
                int pos = k - 1;
                for (int j = k - 2; j >= 0; j--)
                {
                    if (p->sumW > global_topk[j].sumW)
                        pos = j;
                    else
                        break;
                }
                
                for (int j = k - 1; j > pos; j--)
                    global_topk[j] = global_topk[j-1];
                
                global_topk[pos] = *p;
            }
        }
    }
    
    /* ========== Output results ========== */
    Datum values[6];
    bool nulls[6];
    
    for (int i = 0; i < global_count; i++)
    {
        PathState *p = &global_topk[i];
        values[0] = Int32GetDatum(p->A);
        values[1] = Int32GetDatum(p->B);
        values[2] = Int32GetDatum(p->C);
        values[3] = Int32GetDatum(p->D);
        values[4] = Int32GetDatum(p->E);
        values[5] = Int64GetDatum(p->sumW);
        memset(nulls, 0, sizeof(nulls));
        tuplestore_putvalues(rsinfo->setResult, tupdesc, values, nulls);
    }
    
    /* Cleanup */
    hash_destroy(layer_R);
    hash_destroy(layer_U);

    MemoryContextSwitchTo(oldcontext);
    SPI_finish();
    tuplestore_donestoring(rsinfo->setResult);

    PG_RETURN_NULL();
}