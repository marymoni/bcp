#' @useDynLib bcp, .registration=TRUE
NULL

#' Bulk Upload Data Frame into SQL Table
#'
#' @param conn RODBC connection return by odbcDriverConnect() function
#' @param data Data frame to be uploaded
#' @param table_name SQL table name, it must exist in database
#' @param chunk_size How many rows to load in each chunk
#' @param show_progress Logical flag which indicates whether to show data loading progress
#'
#' @export
#'
bcp = function(conn, data, table_name, chunk_size = 5000L, show_progress = TRUE) {

    .Call(R_bcp, attr(conn, "handle_ptr"), data, table_name, as.integer(chunk_size), show_progress)
    invisible()
}
