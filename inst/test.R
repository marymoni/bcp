# CREATE TABLE dbo.DemoTable (
#   A INT NULL,
#   B INT NULL,
#   D DATE NULL,
#   C BIT NULL,
#   E VARCHAR(255),
#   F VARCHAR(255)
# )

library(bcp)

conn = odbcDriverConnect('driver={SQL Server};server=DESKTOP-0U0OJS1\\SQLEXPRESS;database=test;trusted_connection=true')

df = data.frame(
    A = 1:10,
    B = 1:10,
    D = as.Date("2021-01-01") + 1:10,
    C = c(TRUE, FALSE),
    E = paste("str", 1:10),
    F = factor(rep(c("aaa", "bbb"), 5)),
    stringsAsFactors = FALSE
)

bcp(conn, df, "dbo.DemoTable")

bcp(conn, df, "dbo.TableDoesNotExist")

bcp(conn, df, "dbo.TableDoesNotExist", auto_create_table = TRUE)

