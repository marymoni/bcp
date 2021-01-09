#include <Rinternals.h>
#include <R.h>
#include <R_ext/Rdynload.h>

SEXP R_bcp(SEXP x);

R_CallMethodDef callMethods[] = {
    {"R_bcp", (DL_FUNC) &R_bcp, 1},
    {NULL, NULL, 0}
};

void R_init_bcp(DllInfo *info) {
    R_registerRoutines(info, NULL, callMethods, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
}
