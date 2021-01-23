#' @useDynLib bcp, .registration = TRUE
NULL

#' Bulk Upload Data Frame into SQL Table
#'
#' @param conn RODBC connection return by odbcDriverConnect() function
#' @param data Data frame to be uploaded
#' @param table_name SQL table name, it must exist in database
#' @param chunk_size How many rows to load in each chunk
#' @param show_progress Logical flag which indicates whether to show data loading progress
#' @param auto_create_table Logical flag whether automatically create table structure before data uploading
#'
#' @export
#'
bcp = function(conn, data, table_name, chunk_size = 5000L, show_progress = TRUE, auto_create_table = FALSE) {

    if (auto_create_table) {
        create_table_structure(conn, table_name, data)
    }

    .Call(R_bcp, attr(conn, "handle_ptr"), data, table_name, as.integer(chunk_size), show_progress)

    invisible()
}

#' Creates SQL table from R data frame structure
#'
#' @param table_name SQL table name to be created
#' @param data Data frame to be used as prototype for SQL table creation
#' @param dialect SQL dialect for CREATE TABLE statement. As of now only T-SQL supported.
#'
#' @export
#'
create_table_structure = function(conn, table_name, data, dialect = "t-sql") {

    if (dialect != "t-sql") stop("only t-sql dialect is supported for table creation as of now")

    get_column_definition = function(col_name, col_data) {
    
        col_class = class(col_data)
        
        if (any(col_class == "character")) {
            max_len = max(c(nchar(col_data), 1L))
            col_type = sprintf("VARCHAR(%d)", max_len)
        } else if (any(col_class == "integer")) {
            col_type = "INTEGER"
        } else if (any(col_class == "Date")) {
            col_type = "DATE"
        } else if (any(col_class == "numeric")) {
            col_type = "FLOAT"
        } else if (any(col_class == "logical")) {
            col_type = "BIT"
        } else {
            stop(sprintf("unsupported data type '%s' for column '%s'",
                paste(col_class, collapse = ","), col_name))
        }
        
        col_nullable = ifelse(anyNA(col_data), "NULL", "NOT NULL")

        paste(col_name, col_type, col_nullable, collapse = " ")
    }

    sql = paste(
        "CREATE TABLE",
        table_name,
        "(", 
        paste(mapply(get_column_definition, colnames(data), data), collapse = ",\n"),
        ")", collapse = "\n"
    )

    res = sqlQuery(conn, sql)

    if (length(res) > 0) stop(paste(res, collape = "\n"))

    invisible()
}
