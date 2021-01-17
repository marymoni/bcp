#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <stddef.h>
#include <Windows.h>
#include <sql.h>
#include <sqlext.h>

static inline SEXP attr(SEXP vec, SEXP name) {
    
    for (SEXP s = ATTRIB(vec); s != R_NilValue; s = CDR(s)) {        
        if (TAG(s) == name) {
            return CAR(s);
        }        
    }
    
    return R_NilValue;
}

void check_error(SQLRETURN r, char* msg, SQLHANDLE handle, SQLSMALLINT type) {
    
    if (SQL_SUCCEEDED(r)) return;
    
    SQLINTEGER i = 0;
    SQLINTEGER native;
    SQLCHAR state[7];
    SQLCHAR text_info[256];
    SQLSMALLINT len;
    SQLRETURN ret;
    
    Rprintf("ODBC error occured at %s\n", msg);
    
    do {

        ret = SQLGetDiagRec(type, handle, ++i, state, &native, text_info, sizeof(text_info) / sizeof(*text_info), &len);
        
        if (SQL_SUCCEEDED(ret)) {
            Rprintf("SQL ERROR %s: %s\n", state, text_info);
        }
        
    } while (ret == SQL_SUCCESS);
    
    Rf_error("bcp call failed\n");
}

void bcp(SQLHDBC dbc, SEXP r_data, const char* table_name, int chunk_size, int show_progress) {

    if (chunk_size < 1) {
        chunk_size = 1;
    }

    int col_len = LENGTH(r_data);
    if (!col_len) return;
    
    int row_len = LENGTH(VECTOR_ELT(r_data, 0));
    if (!row_len) return;

    SQLLEN col_info[col_len][chunk_size];

    SQLHSTMT stmt;
    SQLRETURN res;
    
    res = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    check_error(res, "Allocate SQL statement handle", dbc, SQL_HANDLE_DBC);

    res = SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER) SQL_CURSOR_DYNAMIC, SQL_IS_UINTEGER);
    check_error(res, "Set cursor attribute", stmt, SQL_HANDLE_STMT);

    res = SQLSetStmtAttr(stmt, SQL_ATTR_CONCURRENCY, (void*) SQL_CONCUR_LOCK, SQL_IS_UINTEGER);
    check_error(res, "Set concurrency attribute", stmt, SQL_HANDLE_STMT);

    res = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) 1, SQL_IS_UINTEGER);
    check_error(res, "Set array row size attribute", stmt, SQL_HANDLE_STMT);

    res = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
    check_error(res, "Set row bind type attribute", stmt, SQL_HANDLE_STMT);
    
    SEXP col_names = attr(r_data, R_NamesSymbol);

    int char_col_len = 0;
    for(int i = 0; i < col_len; i++) {
        if (TYPEOF(VECTOR_ELT(r_data, i)) == STRSXP) {
            char_col_len++;           
        }
    }
    
    int complete = 0;

    while(complete < row_len) {

        int start = complete;
        complete += chunk_size;
        if (complete > row_len) {
            complete = row_len;
        }
        int row_volume = complete - start;
        
        char char_data[char_col_len][row_volume][256];
        int char_data_index;
        
        for(int i = 0; i < col_len; i++) {

            int col_num = i + 1;
            SEXP column = VECTOR_ELT(r_data, i);

            if (TYPEOF(column) == STRSXP) {

                    for(int j = start; j < complete; j++) {
                        const char * d_val = CHAR(STRING_ELT(column, j));
                        strcpy((char*) char_data[char_data_index][j - start], d_val);
                    }

                    res = SQLBindCol(stmt, col_num, SQL_C_CHAR, char_data[char_data_index][0], sizeof(char_data[char_data_index][0]), NULL);
                    check_error(res, "Bind column", stmt, SQL_HANDLE_STMT);
                    char_data_index++;

            } else if (TYPEOF(column) == INTSXP) {

                    int* data_array = INTEGER(column);
                    
                    memset(col_info[i], 0, sizeof(SQLLEN) * row_volume);

                    for(int j = start; j < complete; j++) {
                        if (data_array[j] == NA_INTEGER) {
                            col_info[i][j - start] = SQL_COLUMN_IGNORE;
                        }
                    }

                    res = SQLBindCol(stmt, col_num, SQL_C_LONG, &data_array[start], sizeof(int), col_info[i]);
                    check_error(res, "Bind column", stmt, SQL_HANDLE_STMT);
                
            } else if (TYPEOF(column) == REALSXP) {
                
                    double* data_array = REAL(column);
                    
                    memset(col_info[i], 0, sizeof(SQLLEN) * row_volume);

                    for(int j = start; j < complete; j++) {
                        if (ISNA(data_array[j])) {
                            col_info[i][j - start] = SQL_COLUMN_IGNORE;
                        }
                    }
                
                    res = SQLBindCol(stmt, col_num, SQL_C_DOUBLE, &data_array[start], sizeof(double), col_info[i]);
                    check_error(res, "Bind column", stmt, SQL_HANDLE_STMT);

            } else {
                const char* col_name = CHAR(STRING_ELT(col_names, i));
                Rf_error("Could not load column %s - type not supported\n", col_name);
            }
        }

        // This is to assign SQL statement with target table
        if (!start) {
            char selection[512];
            sprintf(selection, "SELECT TOP 1 * FROM %s", table_name);
            
            res = SQLExecDirect(stmt, (SQLCHAR*) selection, SQL_NTS);
            check_error(res, "Define table name", stmt, SQL_HANDLE_STMT);
        }
        
        res = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) row_volume, SQL_IS_UINTEGER);        
        check_error(res, "Set array row size attribute", stmt, SQL_HANDLE_STMT);
        
        res = SQLBulkOperations(stmt, SQL_ADD);
        check_error(res, "Bulk operation", stmt, SQL_HANDLE_STMT);
        
        if (show_progress) {
            Rprintf("Uploaded %d rows\n", row_volume);
        }
    }

    // TODO: make sure that SQL handle is properly freed in case of errors
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int check_variable_class(const SEXP var_obj, const char *class_name) {

    SEXP obj_classes  = getAttrib(var_obj, R_ClassSymbol);

    if (!isNull(obj_classes)) {
        for(int i = 0; i < length(obj_classes); i++) {
            if (strcmp(CHAR(STRING_ELT(obj_classes, i)), class_name) == 0) {
                return TRUE;
            }
        }
    }

    return FALSE;
}

SEXP R_bcp(SEXP r_handle_ptr, SEXP r_data, SEXP r_table_name, SEXP r_chunk_size, SEXP r_show_progress) {

    if (TYPEOF(r_handle_ptr) != EXTPTRSXP) Rf_error("Incorrect type for connection handle param\n");
    SQLHDBC* dbc_ptr = (SQLHDBC*) R_ExternalPtrAddr(r_handle_ptr);

    if (TYPEOF(r_data) != VECSXP || !check_variable_class(r_data, "data.frame")) Rf_error("Incorrect type for r_data param\n");

    if (TYPEOF(r_table_name) != STRSXP || LENGTH(r_table_name) != 1) Rf_error("Incorrect type or len for table_name param\n");
    const char * table_name = CHAR(STRING_ELT(r_table_name, 0));
    
    if (TYPEOF(r_chunk_size) != INTSXP || LENGTH(r_chunk_size) != 1) Rf_error("Incorrect type or len for chunk_size param\n");
    int chunk_size = INTEGER(r_chunk_size)[0];

    if (TYPEOF(r_show_progress) != LGLSXP || LENGTH(r_show_progress) != 1) Rf_error("Incorrect type or len for show_progress param\n");
    int show_progress = LOGICAL(r_show_progress)[0];

    bcp(*dbc_ptr, r_data, table_name, chunk_size, show_progress);

	return R_NilValue;
}
