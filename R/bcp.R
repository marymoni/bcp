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
#' @param stop_if_error Logical flag whether raise error if BCP operation failed or just return error code
#'
#' @export
#'
bcp = function(conn, data, table_name, chunk_size = 5000L, show_progress = TRUE, auto_create_table = FALSE,
    stop_if_error = TRUE, drop_if_exists = FALSE) {

    if (auto_create_table) {
        create_table_structure(conn, table_name, data, drop_if_exists)
    }

    res = .Call(R_bcp, attr(conn, "handle_ptr"), data, table_name, as.integer(chunk_size), show_progress)
    
    if (stop_if_error && res) stop(sprintf("bcp load failed with exit code %d", res))

    invisible(res)
}

#' Creates SQL table from R data frame structure
#'
#' @param table_name SQL table name to be created
#' @param data Data frame to be used as prototype for SQL table creation
#' @param drop_if_exists Logical flag whether remove existing table and re-create its structure
#' @param dialect SQL dialect for CREATE TABLE statement. As of now only T-SQL supported.
#'
#' @export
#'
create_table_structure = function(conn, table_name, data, drop_if_exists = FALSE, dialect = "t-sql") {

    if (dialect != "t-sql") stop("only t-sql dialect is supported for table creation as of now")

    get_column_definition = function(col_name, col_data) {
    
        col_class = class(col_data)
        
        if (any(col_class == "character" || col_class == "factor")) {
            max_len = max(c(nchar(as.character(col_data)), 1L))
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

    if (drop_if_exists) sqlQuery(conn, sprintf("IF OBJECT_ID('%1$s') IS NOT NULL DROP TABLE %1$s", table_name))

    res = sqlQuery(conn, sql)

    if (length(res) > 0) stop(paste(res, collape = "\n"))

    invisible()
}
