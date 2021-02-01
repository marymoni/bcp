library(bcp)
library(testthat)

conn = odbcDriverConnect('driver={SQL Server};server=DESKTOP-0U0OJS1\\SQLEXPRESS;database=test;trusted_connection=true')

df1 = data.frame(
    A = 1:10000,
    B = 1:10000,
    D = as.Date("2021-01-01") + 1:1000,
    C = c(TRUE, FALSE),
    E = paste("str", 1:10000),
    F = factor(rep(c("aaa", "bbb"), 5)),
    stringsAsFactors = FALSE
)

df2 = data.frame(
    A = 1:10000,
    B = 1:10000,
    D = as.Date("2021-01-01") + 1:1000,    
    stringsAsFactors = FALSE
)

df3 = data.frame(
    A = paste("str", 1:10000),
    B = paste("str", 1:10000),
    D = paste("str", 1:10000),
    stringsAsFactors = FALSE
)

expect_error({ bcp(conn, df1, "dbo.DemoTable", auto_create_table = TRUE, drop_if_exists = TRUE) }, NA)

expect_error({ bcp(conn, df1, "dbo.TableDoesNotExistXYZ", auto_create_table = FALSE) })

expect_error({ bcp(conn, df1[0, ], "dbo.DemoTable", auto_create_table = TRUE, drop_if_exists = TRUE) }, NA)

expect_error({ bcp(conn, df2, "dbo.DemoTable", auto_create_table = TRUE, drop_if_exists = TRUE) }, NA)

expect_error({ bcp(conn, df3, "dbo.DemoTable", auto_create_table = TRUE, drop_if_exists = TRUE) }, NA)
