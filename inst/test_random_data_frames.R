library(bcp)

generate_data_frame = function(nrow, ncol) {

    col_types = list(
        `integer` = function() runif(nrow, -.Machine$integer.max, .Machine$integer.max),
        `double` = function() rnorm(nrow),
        `character` = function() paste("str", as.character(rnorm(nrow))),
        `factor` = function() as.factor(paste("factor", as.character(runif(nrow, 1, 100)))),
        `logical` = function() rnorm(nrow) > 0,
        `Date` = function() as.Date("1970-01-01") + runif(nrow, 0, 25000)
    )

    col_data = lapply(seq_len(ncol), function(i) {
        type_id = runif(1, 1, length(col_types))
        col_types[[type_id]]()
    })
    
    names(col_data) = paste("col", seq_along(col_data), sep = "_")

    res = data.frame(col_data)

    return(res)
}

test_scenarios = list(
    `1` = list(nrow = 100, ncol = 20),
    `2` = list(nrow = 51000, ncol = 30),
    `3` = list(nrow = 1000, ncol = 80),
    `4` = list(nrow = 100000, ncol = 5),
    `5` = list(nrow = 101001, ncol = 1)
)

conn = odbcDriverConnect('driver={SQL Server};server=DESKTOP-0U0OJS1\\SQLEXPRESS;database=test;trusted_connection=true')

for(i in seq_along(test_scenarios)) {

    print(sprintf("scenario %d; nrow = %d; ncol = %d", i, test_scenarios[[i]]$nrow, test_scenarios[[i]]$ncol))

    df = generate_data_frame(nrow = test_scenarios[[i]]$nrow, ncol = test_scenarios[[i]]$ncol)
    
    table_name = sprintf("dbo.DF%d", i)

    time = system.time({
        bcp(conn, df, table_name, auto_create_table = TRUE, drop_if_exists = TRUE)
    })

    print(time)
}

df_perf = generate_data_frame(nrow = 40000, ncol = 60)

sqlQuery(conn, "IF OBJECT_ID('dbo.PerfSqlSaveTest') IS NOT NULL DROP TABLE dbo.PerfSqlSaveTest")

system.time({ bcp(conn, df_perf, "dbo.PerfBcpTest", auto_create_table = TRUE, drop_if_exists = TRUE) })

system.time({ sqlSave(conn, df_perf, "dbo.PerfSqlSaveTest") })

