The sql query
```
SELECT R.A, R.B, S.C, T.D, U.E, R.W1 + S.W2 + T.W3 + U.W4 AS SUMW
FROM R
JOIN S ON R.B = S.B
JOIN T ON S.C = T.C
JOIN U ON T.D = U.D
ORDER BY SUMW DESC
limit K;
```
where all mentioned columns are int4
can be accomplished by
```
SELECT * FROM dp_best_path('R','S','T','U','B','C','D', K);
```
using this extension. Unfortunately argument count is fixed for now.
