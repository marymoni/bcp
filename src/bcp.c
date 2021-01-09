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
    
    SEXP names = attr(r_data, R_NamesSymbol);

    int col_len = LENGTH(r_data);
    int row_len = 0;
    int char_col_len = 0;
    int int_col_len = 0;
    int real_col_len = 0;
    SEXP columns[col_len];
    
    for(int i = 0; i < col_len; i++) {
        columns[i] = VECTOR_ELT(r_data, i);
        row_len = LENGTH(columns[i]);
        
        if (TYPEOF(columns[i]) == STRSXP) {
            char_col_len++;
        } else if (TYPEOF(columns[i]) == INTSXP) {
            int_col_len++;
        } else if (TYPEOF(columns[i]) == REALSXP) {
            real_col_len++;
        } else {
            
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

        SQLLEN int_info[int_col_len][row_volume];
        int int_data_index = 0;
        
        SQLLEN real_info[real_col_len][row_volume];
        int real_data_index = 0;
        
        for(int i = 0; i < col_len; i++) {
            
            int col_num = i + 1;
            
            if (TYPEOF(columns[i]) == STRSXP) {
                
                    for(int j = start; j < complete; j++) {
                        const char * d_val = CHAR(STRING_ELT(columns[i], j));
                        strcpy((char*) char_data[char_data_index][j - start], d_val);
                        
                    }
                
                    res = SQLBindCol(stmt, col_num, SQL_C_CHAR, char_data[char_data_index][0], sizeof(char_data[char_data_index][0]), NULL);
                    check_error(res, "Bind column", stmt, SQL_HANDLE_STMT);
                    char_data_index++;

            } else if (TYPEOF(columns[i]) == INTSXP) {

                    int* data_array = INTEGER(columns[i]);
                    
                    memset(int_info[int_data_index], 0, sizeof(SQLLEN) * row_volume);

                    for(int j = start; j < complete; j++) {
                        if (data_array[j] == NA_INTEGER) {
                            int_info[int_data_index][j - start] = SQL_COLUMN_IGNORE;
                        }
                    }
                
                    res = SQLBindCol(stmt, col_num, SQL_C_LONG, &data_array[start], sizeof(int), int_info[int_data_index]);
                    check_error(res, "Bind column", stmt, SQL_HANDLE_STMT);
                    int_data_index++;
                
            } else if (TYPEOF(columns[i]) == REALSXP) {
                
                    double* data_array = REAL(columns[i]);
                    
                    memset(real_info[real_data_index], 0, sizeof(SQLLEN) * row_volume);

                    for(int j = start; j < complete; j++) {
                        if (ISNA(data_array[j])) {
                            real_info[real_data_index][j - start] = SQL_COLUMN_IGNORE;
                        }
                    }
                
                    res = SQLBindCol(stmt, col_num, SQL_C_DOUBLE, &data_array[start], sizeof(double), real_info[real_data_index]);
                    check_error(res, "Bind column", stmt, SQL_HANDLE_STMT);
                    real_data_index++;
                
            } else {
                const char* col_name = CHAR(STRING_ELT(names, i));
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

SEXP R_bcp(SEXP r_handle_ptr, SEXP r_data, SEXP r_table_name, SEXP r_chunk_size, SEXP r_show_progress) {

    // TODO: add checks for input params
    const char * table_name = CHAR(STRING_ELT(r_table_name, 0));
    int chunk_size = asInteger(r_chunk_size);
    int show_progress = asLogical(r_show_progress);

    SQLHDBC* dbc_ptr = (SQLHDBC*) R_ExternalPtrAddr(r_handle_ptr);

    bcp(*dbc_ptr, r_data, table_name, chunk_size, show_progress);

	return R_NilValue;
}
