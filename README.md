# bcp
Upload R Data Frames by using ODBC Bulk Load API

The package allows to load R data frames / tables into SQL databases by using bulk load ODBC API.

This is faster loader approach than one implemented in RODBC::sqlSave() function.

Simple example

```
library(bcp)

conn = odbcDriverConnect('driver={SQL Server};server=DESKTOP-0U0OJS1\\SQLEXPRESS;database=test;trusted_connection=true')

df = data.frame(A = 1:10, B = 1:10)

bcp(conn, df, "dbo.DemoTable")

```
