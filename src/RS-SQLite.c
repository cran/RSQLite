/* $Id: RS-SQLite.c,v 1.3 2002/09/05 21:03:02 dj Exp dj $
 *
 *
 * Copyright (C) 1999 The Omega Project for Statistical Computing.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "RS-SQLite.h"

/* The macro NA_STRING is a CHRSXP in R but a char * in Splus */
#ifdef USING_R
#  define RS_NA_STRING "<NA>"            /* CHR_EL(NA_STRING,0)  */
#else
#  define RS_NA_STRING NA_STRING
#endif

/* R and S Database Interface to the SQLite embedded SQL engine
 *
 * C Function library which can be used to run SQL queries from
 * inside of Splus5.x, or R.
 * This driver hooks R/S and SQLite and implements the proposed S-DBI
 * generic R/S-database interface 0.2.
 * 
 * SQLite has a (very) minimilst API. In fact it has one, and only
 * one function to exec SQL with no facilities for cursors, data types,
 * result sets, meta data -- nothing.   But it provides ONE and only one
 * hook in the form of a callback function that gets invoked for
 * each row that gets fetched.  So we'll simulate cursors ourseleves
 * through the RS_DBI_resultSet structure -- which is ideal for this.
 * We do this through what we consider the "standard" callback, but
 * clearly there are plenty of oppurtunities to directly hook *arbitrary*
 * R/S computations directly into the database engine.  Cool!
 * Also we need to simulate (fake) exception objects. We do this piggy-
 * backing on the memeber "drvData" of the RS_DBI_connection structure.
 * The exception is a 2-member struct with errorNum and erroMsg.
 *
 * For details on SQLite see www.hwaci.com/sw/sqlite/index.html
 * TODO:
 *    1. Make sure the code is thread-safe, in particular,
 *       we need to remove the PROBLEM ... ERROR macros
 *       in RS_DBI_errorMessage() because it's definetely not 
 *       thread-safe.  But see RS_DBI_setException().
 */


Mgr_Handle *
RS_SQLite_init(s_object *config_params, s_object *reload)
{
  S_EVALUATOR

  /* Currently we can specify the 2 defaults max conns and records per 
   * fetch (this last one can be over-ridden explicitly in the S call to fetch).
   */
  Mgr_Handle *mgrHandle;
  Sint  fetch_default_rec, force_reload, max_con;
  const char *drvName = "SQLite";

  if(GET_LENGTH(config_params)!=2){
     RS_DBI_errorMessage(
        "initialization error: must specify max num of conenctions and default number of rows per fetch", 
        RS_DBI_ERROR);
  } 
  max_con = INT_EL(config_params, 0);
  fetch_default_rec = INT_EL(config_params,1);
  force_reload = LGL_EL(reload,0);

  mgrHandle = RS_DBI_allocManager(drvName, max_con, fetch_default_rec, 
			     force_reload);
  return mgrHandle;
} 

s_object *
RS_SQLite_closeManager(Mgr_Handle *mgrHandle)
{
  S_EVALUATOR

  RS_DBI_manager *mgr;
  s_object *status;

  mgr = RS_DBI_getManager(mgrHandle);
  if(mgr->num_con)
    RS_DBI_errorMessage("there are opened connections -- close them first",
			RS_DBI_ERROR);

  RS_DBI_freeManager(mgrHandle);

  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status,0) = TRUE;
  MEM_UNPROTECT(1);
  return status;
}

/* open a connection with the same parameters used for in conHandle */
Con_Handle *
RS_SQLite_cloneConnection(Con_Handle *conHandle)
{
  S_EVALUATOR

  Mgr_Handle  *mgrHandle;
  RS_DBI_connection  *con;
  RS_SQLite_conParams *conParams;
  s_object    *con_params;
  char   buf1[256];

  /* get connection params used to open existing connection */
  con = RS_DBI_getConnection(conHandle);
  conParams = (RS_SQLite_conParams *) con->conParams;

  mgrHandle = RS_DBI_asMgrHandle(MGR_ID(conHandle));
  
  /* copy dbname and mode into a 2-element character
   * vector to be passed to the RS_SQLite_newConnection() function.
   */
  MEM_PROTECT(con_params = NEW_CHARACTER((Sint) 2));
  SET_CHR_EL(con_params, 0, C_S_CPY(conParams->dbname));
  sprintf(buf1, "%d", (int) conParams->mode);
  SET_CHR_EL(con_params, 1, C_S_CPY(buf1));
  MEM_UNPROTECT(1); 

  return RS_SQLite_newConnection(mgrHandle, con_params);
}

RS_SQLite_conParams *
RS_SQLite_allocConParams(const char *dbname, int mode)
{
  RS_SQLite_conParams *conParams;

  conParams = (RS_SQLite_conParams *) malloc(sizeof(RS_SQLite_conParams));
  if(!conParams){
    RS_DBI_errorMessage("could not malloc space for connection params",
                       RS_DBI_ERROR);
  }
  conParams->dbname = RS_DBI_copyString(dbname);
  conParams->mode = mode;
  return conParams;
}

void
RS_SQLite_freeConParams(RS_SQLite_conParams *conParams)
{
  if(conParams->dbname) 
     free(conParams->dbname);
  /* conParams->mode is an int, thus needs no free */
  free(conParams);   
  conParams = (RS_SQLite_conParams *)NULL;
  return;
}

/* set exception object (allocate memory if needed) */
void
RS_SQLite_setException(RS_DBI_connection *con, int err_no, const char *err_msg)
{
   RS_SQLite_exception *ex;

   ex = (RS_SQLite_exception *) con->drvData;
   if(!ex){    /* brand new exception object */
      ex = (RS_SQLite_exception *) malloc(sizeof(RS_SQLite_exception));
      if(!ex)
         RS_DBI_errorMessage("could not allocate SQLite exception object", 
	              RS_DBI_ERROR);
   }
   else        
      free(ex->errorMsg);      /* re-use existing object */

   ex->errorNum = err_no;
   if(err_msg)
      ex->errorMsg = RS_DBI_copyString(err_msg);
   else
      ex->errorMsg = (char *) NULL;

   con->drvData = (void *) ex;
   return;
}

void
RS_SQLite_freeException(RS_DBI_connection *con)
{
   RS_SQLite_exception *ex = (RS_SQLite_exception *) con->drvData;

   if(!ex) return;
   if(!ex->errorMsg) free(ex->errorMsg);
   free(ex);
   return;
}

Con_Handle *
RS_SQLite_newConnection(Mgr_Handle *mgrHandle, s_object *s_con_params)
{
  S_EVALUATOR

  RS_DBI_connection   *con;
  RS_SQLite_conParams *conParams;
  Con_Handle  *conHandle;
  sqlite      *db_connection;
  char        *dbname = NULL, **errMsg = NULL;
  int         mode;

  if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
    RS_DBI_errorMessage("invalid SQLiteManager", RS_DBI_ERROR);

  /* unpack connection parameters from S object */
  dbname = CHR_EL(s_con_params, 0);
  mode = (Sint) atol(CHR_EL(s_con_params,1)); 
  errMsg = (char **) calloc((size_t) 1, sizeof(char *));

  db_connection = sqlite_open(dbname, mode, errMsg);

  if(!db_connection){
     char buf[256];
     sprintf(buf, "could not connect to dbname \"%s\"\n", dbname);
     free(errMsg[0]);
     free(errMsg);
     RS_DBI_errorMessage(buf, RS_DBI_ERROR);
  }

  /* SQLite connections can only have 1 result set open at a time */  
  conHandle = RS_DBI_allocConnection(mgrHandle, (Sint) 1); 
  con = RS_DBI_getConnection(conHandle);
  if(!con){
    sqlite_close(db_connection);
    RS_DBI_freeConnection(conHandle);
    RS_DBI_errorMessage("could not alloc space for connection object",
                        RS_DBI_ERROR);
  }
  /* save connection parameters in the connection object */
  conParams = RS_SQLite_allocConParams(dbname, mode);
  con->drvConnection = (void *) db_connection;
  con->conParams = (void *) conParams;
  RS_SQLite_setException(con, SQLITE_OK, "OK");

  return conHandle;
}

s_object *
RS_SQLite_closeConnection(Con_Handle *conHandle)
{
  S_EVALUATOR

  RS_DBI_connection *con;
  sqlite *db_connection;
  s_object *status;

  con = RS_DBI_getConnection(conHandle);
  if(con->num_res>0){
    RS_DBI_errorMessage(
     "close the pending result sets before closing this connection",
     RS_DBI_ERROR);
  }
  /* make sure we first free the conParams and SQLite connection from
   * the RS-RBI connection object.
   */
  if(con->conParams){
     RS_SQLite_freeConParams(con->conParams);
     /* we must set con->conParms to NULL (not just free it) to signal 
      * RS_DBI_freeConnection that it is okay to free the connection itself.
      */
     con->conParams = (RS_SQLite_conParams *) NULL;
  }
  db_connection = (sqlite *) con->drvConnection;
  sqlite_close(db_connection);
  con->drvConnection = (void *) NULL;

  RS_SQLite_freeException(con);
  con->drvData = (void *) NULL;
  RS_DBI_freeConnection(conHandle);
  
  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status, 0) = TRUE;
  MEM_UNPROTECT(1);

  return status;
}  
  
Res_Handle *
RS_SQLite_exec(Con_Handle *conHandle, s_object *statement, s_object *s_limit)
{
  S_EVALUATOR

  RS_DBI_connection *con;
  Res_Handle        *rsHandle;
  RS_DBI_resultSet  *res;
  sqlite            *db_connection;
  int      state;
  Sint     res_id, *cbState;
  char     *dyn_statement, **errMsg;

  con = RS_DBI_getConnection(conHandle);
  db_connection = (sqlite *) con->drvConnection;
  dyn_statement = RS_DBI_copyString(CHR_EL(statement,0));

  /* Do we have a pending resultSet in the current connection?  
   * SQLite only allows  one resultSet per connection.
   */
  if(con->num_res>0){
    res_id = (Sint) con->resultSetIds[0]; /* recall, SQLite has only 1 res */
    rsHandle = RS_DBI_asResHandle(MGR_ID(conHandle), 
	                          CON_ID(conHandle),
				  res_id);
    res = RS_DBI_getResultSet(rsHandle);
    if(res->completed != 1){
       free(dyn_statement);
       RS_DBI_errorMessage( 
         "connection with pending rows, close resultSet before continuing",
         RS_DBI_ERROR);
    }
    else 
       RS_SQLite_closeResultSet(rsHandle);
  }

  /* allocate and init a new result set */

  rsHandle =  RS_DBI_allocResultSet(conHandle);
  res = RS_DBI_getResultSet(rsHandle);
  res->completed = (Sint) 0;
  res->statement = dyn_statement;
  cbState = (Sint*) malloc((size_t) 3*sizeof(Sint)); 
  errMsg = (char **) malloc((size_t) 1);
  if(!errMsg || !cbState)
     RS_DBI_errorMessage("could not allocate memory", RS_DBI_ERROR);
  if(GET_LENGTH(s_limit))
     cbState[0] = INT_EL(s_limit,0); /* how many rows we actually fetch */
  else
     cbState[0] = (Sint) -1;         /* fetch all */

  cbState[1] = cbState[2] = -1;      /* to be init in RS_SQLite_stdCallback */
  res->drvData = (void *) cbState;

  state = sqlite_exec(db_connection, 
	              dyn_statement, 
	              RS_SQLite_stdCallback, 
                      (void *) rsHandle,   /* will be passed to the callback*/
		      errMsg);

  if(state!=SQLITE_OK && state!=SQLITE_ABORT){ 
     char buf[512];

     RS_SQLite_setException(con, state, errMsg[0]);
     (void) sprintf(buf, "error in statement: %s", errMsg[0]);
     free(errMsg[0]);
     free(errMsg);
     if(res->drvData){
	free(res->drvData);
        res->drvData = (void *) NULL;
     }
     RS_DBI_freeResultSet(rsHandle);
     if(state != SQLITE_ABORT)
        RS_DBI_errorMessage(buf, RS_DBI_ERROR);
  }

  RS_SQLite_setException(con, state, "OK");
  /* Was statement a SELECT? 
   * In the case of select's res->fields is set by the standard  callback 
   * function, we still need to do some clean up (re-size the cursor cache,
   * etc.)
   */
  if(res->isSelect==1){
     size_t  nrows = (size_t) res->rowsAffected;
     size_t  ncols = (size_t) res->fields->num_fields;
     char    **rows = (char **) res->drvResultSet;

     /* re-size cache to exact size */
     rows = (char **) realloc((void *)rows, nrows*ncols*sizeof(char *));
     res->drvResultSet = (void *) rows;
     res->fields = RS_SQLite_createDataMappings(rsHandle);
     res->rowCount = 0;                  /* fake's cursor's row count */
  }
  else {
     res->isSelect = (Sint) 0;           /* statement is not a select  */
     res->rowsAffected = (Sint) -1;      /* SQLite doesn't report this */
     res->completed = (Sint) 1;          /* BUG: what if query is async?*/
  }

  return rsHandle;
}

/* TODO: change the re-allocation to increasingly allocate bigger
 * chunks.
 *
 * Recall that if the dynamic statement is a select, we fetch all
 * the records into a buffer and stick it into the resultSet slot
 * "drvResultSet";  then, fetch returns rows from this place.
 * We allow to limit the num of rows that go into our cursor by
 * using the "drvData" slot in the DBI result set (we need
 * this to circumvent limitations in SQLite such as not providing
 * field metadata).
 */
int 
RS_SQLite_stdCallback(
      void *rh,       /* resultSet handle  with which we simulate cursors */
      int ncol,       /* num of columns in the result set */
      char **data,    /* ncol-array with the (char) data for the current row */
      char **colNames /* array of column names */
      )
{
   RS_DBI_manager   *mgr;
   Res_Handle       *rsHandle;
   RS_DBI_resultSet *res;
   int   j;
   Sint  *cbState, rows_affected, rows_per_fetch;
   char **rows;       /* ptr into our fake cursor in the resultSet */

   rsHandle = (Res_Handle *) rh;
   res = RS_DBI_getResultSet(rsHandle);
   mgr = RS_DBI_getManager(rsHandle);
   rows_affected = res->rowsAffected;
   rows_per_fetch = mgr->fetch_default_rec;

   /* the state (size of allocated space) across callbacks is kept in cbState.
    * This callback argument keeps track of the size of the space allocated 
    * for the resultSet (which is stored in res->drvResult) 
    * cbState is a triple of Sint's
    *   cbState[0]  absolute max num of rows that the resultSet will hold
    *               (-1 means no limit);
    *   cbState[1]  num or currently allocated rows;
    *   cbState[2]  num of incremental rows (this is be doubled each time
    *               we go thru the re-allocation step).
    */

   cbState = (Sint *) res->drvData; 

   if(rows_affected<0){
      /* if res->rowsAffected is -1 then resultSet is un-initialized.
       * Note that we get here only for select statements, thus we 
       * record the fact here.
       */
      res->rowsAffected = rows_affected = (Sint) 0; 
      res->isSelect = (Sint) 1;    
      RS_SQLite_initFields(res, ncol, colNames);
      cbState[1] = cbState[2] = rows_per_fetch; 
      rows = (char **) calloc((size_t) cbState[1]*ncol, sizeof(char *));
      res->drvResultSet = (void *) rows;
   }
   else {
      rows = (char **) res->drvResultSet;
   }

   /* reach absolute maximum number of rows? */
   if(cbState[0]>0 && rows_affected>=cbState[0])
      return SQLITE_ABORT;  

   if(cbState[1] == rows_affected){ /* have we exhausted allocated space? */
      cbState[2] *= 2;              /* add twice increment */
      cbState[1] += cbState[2];
      rows = (char **) realloc((void *) rows, 
                               (size_t) ncol*cbState[1]*sizeof(char *));
      if(!rows) {
	 res->drvResultSet = (void *) NULL;
	 RS_DBI_freeResultSet(rsHandle);
	 return SQLITE_NOMEM;
      }
      else
         res->drvResultSet = (void *) rows;
   }
   
   /* we just copy each column to the row buffer in the result set */
   for(j=0; j<ncol; j++){
      if(data[j]==0) 
	 rows[rows_affected*ncol+j] = RS_DBI_copyString(RS_NA_STRING);
      else
         rows[rows_affected*ncol+j] = RS_DBI_copyString(data[j]);
   }
   res->rowsAffected += (Sint) 1;

   return 0;
}

void RS_SQLite_initFields(RS_DBI_resultSet *res, int ncol, char **colNames) 
{
   RS_DBI_fields    *flds;
   Sint j;

   if(res->isSelect!=1 || res->fields){
         return;      /* no output or already initialized -- should we warn?*/
   }
   flds = RS_DBI_allocFields((Sint) ncol); /* BUG: mem leak if this fails */
   flds->num_fields = ncol;
   for(j=0; j<ncol; j++){
      if(colNames[j])
         flds->name[j] = RS_DBI_copyString(colNames[j]);
      else
         flds->name[j] = RS_DBI_copyString(RS_NA_STRING);
      flds->type[j] = (Sint) -1;     /* SQLite stores everything as varchar */
      flds->length[j] = (Sint) -1;   /* unknown */
      flds->precision[j] = (Sint) -1;  
      flds->scale[j] = (Sint) -1;      
      flds->nullOk[j] = (Sint) -1;   /* actually we may be able to get(?) */
      flds->isVarLength[j] = (Sint) -1;
      flds->Sclass[j] = (Sint) -1;
   }

   /* finally we initialized the data buffer where we'll keep in the result */
   res->drvResultSet = (void *) NULL;     /* we'll use this as a buffer */
   res->fields = (void *) flds;

   return;
}

RS_DBI_fields *
RS_SQLite_createDataMappings(Res_Handle *rsHandle)
{
   /* SQLite recognizes no other data than character. */

   RS_DBI_resultSet  *res;
   RS_DBI_fields     *flds;
   int j;

   res = RS_DBI_getResultSet(rsHandle);
   flds = res->fields;
   for(j=0; j< flds->num_fields; j++){
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->type[j] = SQL92_TYPE_CHAR_VAR;
   }
   return flds;
}

/* we will return a data.frame with character data and then invoke
 * the .Internal(type.convert(...)) as in read.table in the 
 * calling R/S function.  Grrr!
 */
s_object *      /* data.frame */
RS_SQLite_fetch(s_object *rsHandle, s_object *max_rec)
{
  S_EVALUATOR

  RS_DBI_resultSet *res;
  RS_DBI_fields    *flds;
  char             **rows;          /* ptr to our cache */
  s_object  *output;
  int    i, j, k, null_item;
  Stype  *Sclass; 
  Sint   n, completed, num_rec, rec_left;
  int    num_fields;

  res = RS_DBI_getResultSet(rsHandle);
  if(res->isSelect != 1){
     RS_DBI_errorMessage("resultSet does not correspond to a SELECT statement",
	   RS_DBI_WARNING);
     return S_NULL_ENTRY;
  }
  rec_left = res->rowsAffected - res->rowCount;
  if(rec_left==0){                         /* are we done yet? */
     res->completed = (Sint) 1;            /* should we also free rows? */
     return S_NULL_ENTRY;                  /* no more to fetch */
  }

  rows = (char **) res->drvResultSet;      /* this is our cursor */
  if(!rows)
     RS_DBI_errorMessage("corrupt SQLite resultSet, missing row cache", 
	   RS_DBI_ERROR);
  flds = res->fields;
  if(!flds)
     RS_DBI_errorMessage("corrupt SQLite resultSet, missing fieldDescription",
	   RS_DBI_ERROR);

  n = INT_EL(max_rec,0);                    /* return up to n recs */
  if(n<=0)
     num_rec = rec_left;                    /* return everything */
  else
     num_rec = (rec_left <= n)? rec_left : n;

  num_fields = flds->num_fields;
  MEM_PROTECT(output = NEW_LIST((Sint) num_fields));
  RS_DBI_allocOutput(output, flds, num_rec, 0);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc output list",
			RS_DBI_ERROR);
#endif
  
  Sclass = flds->Sclass;

  for(i = 0; i<num_rec; i++){

      for(j = 0; j < num_fields; j++){

          k = (res->rowCount + i) * num_fields + j; 
          null_item = (rows[k]==0) ? 1 : 0;
  
          /* SQLite has only chars, nevertheless I believe it'll support
           * some basic types (the SQLite odbc seems to be pushing for this).
           */
          switch((int)Sclass[j]){
          case INTEGER_TYPE:
	    if(null_item)
	      NA_SET(&(LST_INT_EL(output,j,i)), INTEGER_TYPE);
	    else
	      LST_INT_EL(output,j,i) = (Sint) atol(rows[k]);
	    break;
          case CHARACTER_TYPE:
	    if(null_item)
#ifdef USING_R
	      SET_LST_CHR_EL(output,j,i, C_S_CPY(RS_NA_STRING));
#else
	      NA_CHR_SET(LST_CHR_EL(output,j,i));
#endif
	    else 
	      SET_LST_CHR_EL(output,j,i,C_S_CPY(rows[k]));
	    break;
          case NUMERIC_TYPE:
	    if(null_item)
	      NA_SET(&(LST_NUM_EL(output,j,i)), NUMERIC_TYPE);
	    else
	      LST_NUM_EL(output,j,i) = (double) atof(rows[k]);
	    break;
#ifndef USING_R
          case SINGLE_TYPE:
	    if(null_item)
	      NA_SET(&(LST_FLT_EL(output,j,i)), SINGLE_TYPE);
	    else
	      LST_FLT_EL(output,j,i) = (float) atof(rows[k]);
	    break;
#endif
          default:  /* error, but we'll try the field as character (!)*/
	    if(null_item)
#ifdef USING_R
	      SET_LST_CHR_EL(output,j,i, C_S_CPY(RS_NA_STRING));
#else
	      NA_CHR_SET(LST_CHR_EL(output,j,i));
#endif
	    else {
	        char warn[64];
	        (void) sprintf(warn, 
			       "unrecognized field type %d in column %d",
			       (int) Sclass[j], (int) j);
	        RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	        SET_LST_CHR_EL(output,j,i,C_S_CPY(rows[k]));
	      }
            break;
        } 
    } 
  }
  
  if(i < num_rec)
     RS_DBI_errorMessage("error while fetching rows", RS_DBI_WARNING);
  res->rowCount += (Sint) i;
  completed = (res->rowCount < res->rowsAffected) ? 0 : 1;
  /* TODO: if completed, should we free the cache now or should we
   * allow seek operations on it?  The DBI has not defined these.
   */
  res->completed = (int) completed;

  MEM_UNPROTECT(1);
  return output;
}

/* return a 2-elem list with the last exception number and exception message on a given connection.
 * NOTE: RS_SQLite_getException() is meant to be used mostly directory R.
 */
s_object * 
RS_SQLite_getException(s_object *conHandle)
{
  S_EVALUATOR

  s_object  *output;
  RS_DBI_connection   *con;
  RS_SQLite_exception *err;
  Sint  n = 2;
  char *exDesc[] = {"errorNum", "errorMsg"};
  Stype exType[] = {INTEGER_TYPE, CHARACTER_TYPE};
  Sint  exLen[]  = {1, 1};

  con = RS_DBI_getConnection(conHandle);
  if(!con->drvConnection)
    RS_DBI_errorMessage("internal error: corrupt connection handle",
			RS_DBI_ERROR);
  output = RS_DBI_createNamedList(exDesc, exType, exLen, n);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not allocate named list",
			RS_DBI_ERROR);
#endif
  err = (RS_SQLite_exception *) con->drvData;
  LST_INT_EL(output,0,0) = (Sint) err->errorNum;
  SET_LST_CHR_EL(output,1,0,C_S_CPY(err->errorMsg));

  return output;
}

s_object *
RS_SQLite_closeResultSet(s_object *resHandle)
{
  S_EVALUATOR 

  RS_DBI_resultSet *result;
  s_object *status;
  char     **rows;
  Sint     i, j, nrows, ncols;
  
  result = RS_DBI_getResultSet(resHandle);
  rows = (char **) result->drvResultSet;
  if(rows){
     nrows = result->rowsAffected;
     ncols = result->fields->num_fields;
     for(i=0; i<nrows; i++)
        for(j=0; j<ncols; j++)
           if(rows[i*ncols+j]) free(rows[i*ncols+j]);
     free(rows);
  }
  /* need to NULL drvResultSet, otherwise can't free the rsHandle */
  result->drvResultSet = (void *) NULL;
  if(result->drvData)
     free(result->drvData);
  result->drvData = (void *) NULL;
  RS_DBI_freeResultSet(resHandle);

  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status, 0) = TRUE;
  MEM_UNPROTECT(1);

  return status;
}

s_object *
RS_SQLite_managerInfo(Mgr_Handle *mgrHandle)
{
  S_EVALUATOR

  RS_DBI_manager *mgr;
  s_object *output;
  Sint i, num_con, max_con, *cons, ncon;
  Sint j, n = 8;
  char *mgrDesc[] = {"drvName",   "connectionIds", "fetch_default_rec",
                     "managerId", "length",        "num_con", 
                     "counter",   "clientVersion"};
  Stype mgrType[] = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE, 
                     INTEGER_TYPE,   INTEGER_TYPE, INTEGER_TYPE, 
                     INTEGER_TYPE,   CHARACTER_TYPE};
  Sint  mgrLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};
  
  mgr = RS_DBI_getManager(mgrHandle);
  if(!mgr)
    RS_DBI_errorMessage("driver not loaded yet", RS_DBI_ERROR);
  num_con = (Sint) mgr->num_con;
  max_con = (Sint) mgr->length;
  mgrLen[1] = num_con;

  output = RS_DBI_createNamedList(mgrDesc, mgrType, mgrLen, n);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list", 
			RS_DBI_ERROR);
  j = (Sint) 0;
  if(mgr->drvName)
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(mgr->drvName));
  else 
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(""));

  cons = (Sint *) S_alloc((long)max_con, (int)sizeof(Sint));
  ncon = RS_DBI_listEntries(mgr->connectionIds, mgr->length, cons);
  if(ncon != num_con){
    RS_DBI_errorMessage(
	  "internal error: corrupt RS_DBI connection table",
	  RS_DBI_ERROR);
  }
  for(i = 0; i < num_con; i++)
    LST_INT_EL(output, j, i) = cons[i];
  j++;
  LST_INT_EL(output,j++,0) = mgr->fetch_default_rec;
  LST_INT_EL(output,j++,0) = mgr->managerId;
  LST_INT_EL(output,j++,0) = mgr->length;
  LST_INT_EL(output,j++,0) = mgr->num_con;
  LST_INT_EL(output,j++,0) = mgr->counter;
  SET_LST_CHR_EL(output,j++,0,C_S_CPY(SQLITE_VERSION));

  return output;
}

s_object *
RS_SQLite_connectionInfo(Con_Handle *conHandle)
{
  S_EVALUATOR
  
  RS_SQLite_conParams *conParams;
  RS_DBI_connection  *con;
  s_object   *output;
  Sint       i, n = 7, *res, nres;
  char *conDesc[] = {"host", "user", "dbname", "conType",
		     "serverVersion", "threadId", "rsId"};
  Stype conType[] = {CHARACTER_TYPE, CHARACTER_TYPE, CHARACTER_TYPE,
		      CHARACTER_TYPE, CHARACTER_TYPE,
		      INTEGER_TYPE, INTEGER_TYPE};
  Sint  conLen[]  = {1, 1, 1, 1, 1, 1, 1};

  con = RS_DBI_getConnection(conHandle);
  conLen[6] = con->num_res;         /* num of open resultSets */
  output = RS_DBI_createNamedList(conDesc, conType, conLen, n);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
			RS_DBI_ERROR);
#endif
  conParams = (RS_SQLite_conParams *) con->conParams;
  SET_LST_CHR_EL(output,0,0,C_S_CPY("localhost"));
  SET_LST_CHR_EL(output,1,0,C_S_CPY(getlogin()));
  SET_LST_CHR_EL(output,2,0,C_S_CPY(conParams->dbname));
  SET_LST_CHR_EL(output,3,0,C_S_CPY("direct"));
  SET_LST_CHR_EL(output,4,0,C_S_CPY(SQLITE_VERSION));

  LST_INT_EL(output,5,0) = (Sint) -1;

  res = (Sint *) S_alloc( (long) con->length, (int) sizeof(Sint));
  nres = RS_DBI_listEntries(con->resultSetIds, con->length, res);
  if(nres != con->num_res){
    RS_DBI_errorMessage(
	  "internal error: corrupt RS_DBI resultSet table",
	  RS_DBI_ERROR);
  }
  for( i = 0; i < con->num_res; i++){
    LST_INT_EL(output,6,i) = (Sint) res[i];
  }

  return output;

}
s_object *
RS_SQLite_resultSetInfo(Res_Handle *rsHandle)
{
  S_EVALUATOR

  RS_DBI_resultSet   *result;
  s_object  *output, *flds;
  Sint  n = 6;
  char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
		     "rowCount", "completed", "fieldDescription"};
  Stype rsType[]  = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE,
		     INTEGER_TYPE,   INTEGER_TYPE, LIST_TYPE};
  Sint  rsLen[]   = {1, 1, 1, 1, 1, 1};

  result = RS_DBI_getResultSet(rsHandle);
  if(result->fields)
    flds = RS_DBI_getFieldDescriptions(result->fields);
  else
    flds = S_NULL_ENTRY;

  output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
			RS_DBI_ERROR);
  SET_LST_CHR_EL(output,0,0,C_S_CPY(result->statement));
  LST_INT_EL(output,1,0) = result->isSelect;
  LST_INT_EL(output,2,0) = result->rowsAffected;
  LST_INT_EL(output,3,0) = result->rowCount;
  LST_INT_EL(output,4,0) = result->completed;
  if(flds != S_NULL_ENTRY)
     SET_ELEMENT(LST_EL(output, 5), (Sint) 0, flds);

  return output;
}

s_object *
RS_SQLite_typeNames(s_object *typeIds)
{
  s_object *typeNames;
  Sint n;
  Sint *typeCodes;
  int i;
  char *s;
  
  n = LENGTH(typeIds);
  typeCodes = INTEGER_DATA(typeIds);
  MEM_PROTECT(typeNames = NEW_CHARACTER(n));
  for(i = 0; i < n; i++) {
    s = RS_DBI_getTypeName(typeCodes[i], RS_SQLite_fieldTypes);
    SET_CHR_EL(typeNames, i, C_S_CPY(s));
  }
  MEM_UNPROTECT(1);
  return typeNames;
}
