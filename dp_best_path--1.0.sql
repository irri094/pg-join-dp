CREATE OR REPLACE FUNCTION dp_best_path(
    r_table text,
    s_table text, 
    t_table text,
    u_table text,
    r_join_col text,
    s_join_col text,
    t_join_col text,
    k integer       
)
RETURNS TABLE (
    a int,
    b int,
    c int,
    d int,
    e int,
    sumw int
)
AS '$libdir/dp_best_path', 'dp_best_path'
LANGUAGE C STRICT;
