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

conn = odbcDriverConnect('driver={SQL Server};server=DESKTOP-0U0OJS1\\SQLEXPRESS;database=test;trusted_connection=true')

df1 = generate_data_frame(nrow = 100, ncol = 20)

bcp(conn, df1, "dbo.DF1", auto_create_table = TRUE, drop_if_exists = TRUE)

df2 = generate_data_frame(nrow = 51000, ncol = 30)

bcp(conn, df1, "dbo.DF1", auto_create_table = TRUE, drop_if_exists = TRUE)
