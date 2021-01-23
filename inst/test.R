# CREATE TABLE dbo.DemoTable (
#   A INT NULL,
#   B INT NULL
# )

library(bcp)

conn = odbcDriverConnect('driver={SQL Server};server=DESKTOP-0U0OJS1\\SQLEXPRESS;database=test;trusted_connection=true')

df = data.frame(A = 1:10, B = 1:10)

bcp(conn, df, "dbo.DemoTable")

bcp(conn, df, "dbo.TableDoesNotExist")

bcp(conn, df, "dbo.TableDoesNotExist", auto_create_table = TRUE)

