| Table              |   MedInc |   Δ MedInc |   AveRooms |   Δ AveRooms |   Population |   Δ Population |   Rows |   Δ Rows | Reason                                     | Configurations flag   |
|:-------------------|---------:|-----------:|-----------:|-------------:|-------------:|---------------:|-------:|---------:|:-------------------------------------------|:----------------------|
| california housing |    20640 |          0 |      20640 |            0 |         3888 |              0 |  20640 |        0 | Raw california housing data                |                       |
| california housing |    16245 |      -4395 |      16245 |        -4395 |         3776 |           -112 |  16245 |    -4395 | Select in-scope bedrooms                   | 1.0                   |
| virginia housing   |    20640 |          0 |      20640 |            0 |         3888 |              0 |  20640 |        0 | Load Virginia's housing data               | n/a                   |
| california housing |    20925 |       4680 |      20925 |         4680 |         3888 |            112 |  20925 |     4680 | Join houses w/ 0.5 to 1.0 average bedrooms | True                  |
| california housing |     1669 |     -19256 |       1669 |       -19256 |         1170 |          -2718 |   1669 |   -19256 | Maximum two occupants                      | 2.0                   |
| california housing |     1564 |       -105 |       1564 |         -105 |         1110 |            -60 |   1564 |     -105 | House age range                            | [10, 80]              |
