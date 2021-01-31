#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <stddef.h>
#include <Windows.h>
#include <sql.h>
#include <sqlext.h>
#include <time.h>

#define BOOL_TYPE 'B'
#define CHAR_TYPE 'C'
#define DATE_TYPE 'D'
#define REAL_TYPE 'R'
#define INT_TYPE 'I'
#define FACT_TYPE 'F'

#define MAX_CHAR_LEN 255

#define EXIT_CODE_SUCCESS 0
#define EXIT_CODE_MEM_ALLOC_FAIL 1
#define EXIT_CODE_SQL_ERROR 2
#define EXIT_CODE_UNKNOWN_TYPE 2

typedef struct {
    int length;
    char* values;
} FACTOR_LEVELS;

static inline SEXP attr(SEXP vec, SEXP name) {
    
    for (SEXP s = ATTRIB(vec); s != R_NilValue; s = CDR(s)) {        
        if (TAG(s) == name) {
            return CAR(s);
        }        
    }

    return R_NilValue;
}

int check_sql_error(SQLRETURN r, char* msg, SQLHANDLE handle, SQLSMALLINT type) {
    
    if (SQL_SUCCEEDED(r)) return EXIT_CODE_SUCCESS;
    
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
    
    return EXIT_CODE_SQL_ERROR;
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

// TODO: fix 2038+ days conversion
void convert_to_sql_date(int r_date, DATE_STRUCT *sql_date) {

    // convert days since epoch to seconds since epoch
    time_t unix_time = r_date * 86400;

    struct tm * ptm;
    ptm = gmtime(&unix_time);
    
    sql_date->day = ptm->tm_mday;
    sql_date->month = ptm->tm_mon + 1;
    sql_date->year = ptm->tm_year + 1900;
}

int bcp(SQLHDBC dbc, SEXP r_data, const char* table_name, int chunk_size, int show_progress) {

    if (chunk_size < 1) {
        chunk_size = 1;
    }
    
    int exit_code = EXIT_CODE_SUCCESS;

    int col_len = LENGTH(r_data);
    int row_len = LENGTH(VECTOR_ELT(r_data, 0));

    if (!col_len || !row_len) return exit_code;  

    SQLLEN col_info[col_len][chunk_size];
    unsigned char col_types[col_len];
    char *char_data = NULL;
    DATE_STRUCT* date_data = NULL;
    FACTOR_LEVELS* factors = NULL;
    
    SQLHSTMT stmt = NULL;
    SQLRETURN res;
    
    res = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if ((exit_code = check_sql_error(res, "Allocate SQL statement handle", dbc, SQL_HANDLE_DBC))) goto end;

    res = SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER) SQL_CURSOR_DYNAMIC, SQL_IS_UINTEGER);
    if ((exit_code = check_sql_error(res, "Set cursor attribute", stmt, SQL_HANDLE_STMT))) goto end;

    res = SQLSetStmtAttr(stmt, SQL_ATTR_CONCURRENCY, (void*) SQL_CONCUR_LOCK, SQL_IS_UINTEGER);
    if ((exit_code = check_sql_error(res, "Set concurrency attribute", stmt, SQL_HANDLE_STMT))) goto end;

    res = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) 1, SQL_IS_UINTEGER);
    if ((exit_code = check_sql_error(res, "Set array row size attribute", stmt, SQL_HANDLE_STMT))) goto end;

    res = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
    if ((exit_code = check_sql_error(res, "Set row bind type attribute", stmt, SQL_HANDLE_STMT))) goto end;
    
    SEXP col_names = attr(r_data, R_NamesSymbol);

    int char_col_len = 0;
    int date_col_len = 0;
    
    factors = calloc(col_len, sizeof(FACTOR_LEVELS));
    if (!factors) {
        exit_code = EXIT_CODE_MEM_ALLOC_FAIL;
        goto end;
    }

    for(int i = 0; i < col_len; i++) {
        SEXP col_data = VECTOR_ELT(r_data, i);
        if (TYPEOF(col_data) == STRSXP) {

            char_col_len++;
            col_types[i] = CHAR_TYPE;

        } else if (TYPEOF(col_data) == REALSXP) {

            if (check_variable_class(col_data, "Date")) {
                col_types[i] = DATE_TYPE;
                date_col_len++;
            } else {
                col_types[i] = REAL_TYPE;
            }

        } else if (TYPEOF(col_data) == INTSXP) {

            SEXP levels = getAttrib(col_data, install("levels"));
            if (isNull(levels)) {

                col_types[i] = INT_TYPE;

            } else {

                col_types[i] = FACT_TYPE;                
                int level_len = LENGTH(levels);

                factors[i].values = calloc(level_len, sizeof(char) * MAX_CHAR_LEN);
                if (factors[i].values == NULL) {
                    exit_code = EXIT_CODE_MEM_ALLOC_FAIL;
                    goto end;
                }

                factors[i].length = level_len;

                for(int k = 0; k < level_len; k++) {
                    const char* factor_value = CHAR(STRING_ELT(levels, k));
                    strncpy(factors[i].values + k * MAX_CHAR_LEN, factor_value, MAX_CHAR_LEN);
                }

                char_col_len++;
            }

        } else if (TYPEOF(col_data) == LGLSXP) {

            col_types[i] = BOOL_TYPE;

        } else {

            const char* col_name = CHAR(STRING_ELT(col_names, i));
            Rprintf("Could not load column %s - type %d not supported\n", col_name, TYPEOF(col_data));
            exit_code = EXIT_CODE_UNKNOWN_TYPE;
            goto end;

        }
    }

    int char_data_size = MAX_CHAR_LEN * char_col_len * sizeof(char) * chunk_size;
    
    if (char_data_size) {
        char_data = malloc(char_data_size);
        if (!char_data) {
            exit_code = EXIT_CODE_MEM_ALLOC_FAIL;
            goto end;
        }
    }

    if (date_col_len) {        
        date_data = calloc(chunk_size * date_col_len, sizeof(DATE_STRUCT));
        if (!date_data) {
            exit_code = EXIT_CODE_MEM_ALLOC_FAIL;
            goto end;
        }
    }

    int complete = 0;

    while(complete < row_len) {

        int date_data_index = 0;
        int char_data_used = 0;

        int start = complete;
        complete += chunk_size;
        if (complete > row_len) {
            complete = row_len;
        }
        int row_volume = complete - start;

        for(int i = 0; i < col_len; i++) {

            int col_num = i + 1;

            SEXP column = VECTOR_ELT(r_data, i);
            
            memset(col_info[i], 0, sizeof(SQLLEN) * row_volume);

            if (col_types[i] == CHAR_TYPE) {
                
                const char* char_data_start = (char*) (char_data + char_data_used);
            
                for(int j = start; j < complete; j++) {
                    
                    const char * d_val = CHAR(STRING_ELT(column, j));
                   
                    col_info[i][j - start] = SQL_COLUMN_IGNORE;
                    col_info[i][j - start] = SQL_NTS;

                    strcpy((char*) (char_data + char_data_used), d_val);
                    char_data_used += MAX_CHAR_LEN;
                }
                
                res = SQLBindCol(stmt, col_num, SQL_C_CHAR, char_data_start, MAX_CHAR_LEN, col_info[i]);
                if ((exit_code = check_sql_error(res, "Bind column", stmt, SQL_HANDLE_STMT))) goto end;

            } else if (col_types[i] == REAL_TYPE || col_types[i] == DATE_TYPE) {

                double* num_data_array = REAL(column);

                for(int j = start; j < complete; j++) {
                    if (ISNA(num_data_array[j])) {
                        col_info[i][j - start] = SQL_COLUMN_IGNORE;
                    }
                }

                if (col_types[i] == DATE_TYPE) {
                    for(int j = start; j < complete; j++) {
                        convert_to_sql_date(num_data_array[j], &date_data[(date_data_index * chunk_size) + j - start]);
                    }
                    res = SQLBindCol(stmt, col_num, SQL_C_TYPE_DATE, &date_data[date_data_index * chunk_size],
                        sizeof(DATE_STRUCT) * row_volume, col_info[i]);
                    date_data_index++;
                } else {
                    res = SQLBindCol(stmt, col_num, SQL_C_DOUBLE, &num_data_array[start],
                        sizeof(double), col_info[i]);
                }

                if ((exit_code = check_sql_error(res, "Bind column", stmt, SQL_HANDLE_STMT))) goto end;

            } else if (col_types[i] == FACT_TYPE) {

                int step = MAX_CHAR_LEN * sizeof(char);

                int* int_data_array = INTEGER(column);
                const char* char_data_start = (char*) (char_data + char_data_used);

                for (int j = start; j < complete; j++) {
                    
                    if (int_data_array[j] == NA_INTEGER) {
                        col_info[i][j - start] = SQL_COLUMN_IGNORE;
                    } else {
                        col_info[i][j - start] = SQL_NTS;
                        int k = int_data_array[j] - 1;
                        strcpy((char*)char_data + char_data_used, factors[i].values + k * step);                        
                    }
                    char_data_used += MAX_CHAR_LEN;
                    
                    res = SQLBindCol(stmt, col_num, SQL_C_CHAR, char_data_start, MAX_CHAR_LEN,
                        col_info[i]);
                    if ((exit_code = check_sql_error(res, "Bind column", stmt, SQL_HANDLE_STMT))) goto end;
                }

            } else if (col_types[i] == INT_TYPE) {
            
                int* int_data_array = INTEGER(column);

                for(int j = start; j < complete; j++) {
                    if (int_data_array[j] == NA_INTEGER) {
                        col_info[i][j - start] = SQL_COLUMN_IGNORE;
                    }
                }

                res = SQLBindCol(stmt, col_num, SQL_C_LONG, &int_data_array[start], sizeof(int), col_info[i]);
                if ((exit_code = check_sql_error(res, "Bind column", stmt, SQL_HANDLE_STMT))) goto end;
                
            } else if (col_types[i] == BOOL_TYPE) {
                
                int* bool_data_array = LOGICAL(column);

                for(int j = start; j < complete; j++) {
                    if (bool_data_array[j] == NA_LOGICAL) {
                        col_info[i][j - start] = SQL_COLUMN_IGNORE;
                    }
                }

                res = SQLBindCol(stmt, col_num, SQL_C_LONG, &bool_data_array[start], sizeof(int), col_info[i]);
                if ((exit_code = check_sql_error(res, "Bind column", stmt, SQL_HANDLE_STMT))) goto end;
            }
        }

        // This is to assign SQL statement with target table
        if (!start) {
            char selection[512];
            sprintf(selection, "SELECT TOP 1 * FROM %s", table_name);
            
            res = SQLExecDirect(stmt, (SQLCHAR*) selection, SQL_NTS);
            if ((exit_code = check_sql_error(res, "Define table name", stmt, SQL_HANDLE_STMT))) goto end;
        }
        
        res = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) row_volume, SQL_IS_UINTEGER);        
        if ((exit_code = check_sql_error(res, "Set array row size attribute", stmt, SQL_HANDLE_STMT))) goto end;
        
        res = SQLBulkOperations(stmt, SQL_ADD);
        if ((exit_code = check_sql_error(res, "Bulk operation", stmt, SQL_HANDLE_STMT))) goto end;
        
        if (show_progress) {
            Rprintf("Uploaded %d rows\n", row_volume);
        }
    }

    end:

        if (char_data) {
            free(char_data);
        }
        
        if (date_data) {
            free(date_data);
        }

        if (factors) {
            for(int i = 0; i < col_len; i++) {
                if (factors[i].values) {
                    free(factors[i].values);
                }
            }
            free(factors);
        }

        if (stmt) {
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        }
        
        return exit_code;
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

    int exit_code = bcp(*dbc_ptr, r_data, table_name, chunk_size, show_progress);

	return ScalarInteger(exit_code);
}
