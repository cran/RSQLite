## This file defines some functions that mimic S4 functionality,
## namely:  new, as, show.

usingR <- function(major=0, minor=0){
  if(is.null(version$language))
    return(FALSE)
  if(version$language!="R")
    return(FALSE)
  version$major>=major && version$minor>=minor
}

## constant holding the appropriate error class returned by try() 
if(usingR()){
  ErrorClass <- "try-error"
} else {
  ErrorClass <- "Error"  
}

as <- function(object, classname)
{
  get(paste("as", as.character(classname), sep = "."))(object)
}

new <- function(classname, ...)
{
  if(!is.character(classname))
    stop("classname must be a character string")
  class(classname) <- classname
  UseMethod("new")
}

new.default <- function(classname, ...)
{
  structure(list(...), class = unclass(classname))
}

show <- function(object, ...)
{
  UseMethod("show")
}

show.default <- function(object)
{
   print(object)
   invisible(NULL)
}

oldClass <- class

"oldClass<-" <- function(x, value)
{
  class(x) <- value
  x
}
## $Id$
##
## DBI.S  Database Interface Definition
## For full details see http://www.omegahat.org
##
## Copyright (C) 1999,2000 The Omega Project for Statistical Computing.
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU Lesser General Public
## License as published by the Free Software Foundation; either
## version 2 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## Lesser General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public
## License along with this library; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
##

## Define all the classes and methods to be used by an implementation
## of the RS-DataBase Interface.  All these classes are virtual
## and each driver should extend them to provide the actual implementation.
## See the files Oracle.S and MySQL.S for the Oracle and MySQL 
## S implementations, respectively.  The support files (they have "support"
## in their names) for code that is R-friendly and may be easily ported
## to R. 

## Class: dbManager 
## This class identifies the DataBase Management System (oracle, informix, etc)

dbManager <- function(obj, ...)
{
  do.call(as.character(obj), list(...))
}
load <- function(mgr, ...)
{
  UseMethod("load")
}
unload <- function(mgr, ...)
{
  UseMethod("unload")
}
getManager <- function(obj, ...)
{
  UseMethod("getManager")
}
getConnections <- function(mgr, ...)
{
  UseMethod("getConnections")
}
## Class: dbConnection

dbConnect <- function(mgr, ...)
{
  UseMethod("dbConnect")
}
dbExecStatement <- function(con, statement, ...)
{
  UseMethod("dbExecStatement")
}
dbExec <- function(con, statement, ...)
{
  UseMethod("dbExec")
}
commit <- function(con, ...)
{
  UseMethod("commit")
}
rollback <- function(con, ...)
{
  UseMethod("rollback")
}
callProc <- function(con, ...)
{
  UseMethod("callProc")
}
close.dbConnection <- function(con, ...)
{
  stop("close for dbConnection objects needs to be written")
}

getResultSets <- function(con, ...)
{
   UseMethod("getResultSets")
}
getException <- function(con, ...)
{
   UseMethod("getException")
}

## close is already a generic function in R

## Class: dbResult
## This is the base class for arbitrary results from TDBSM (INSERT,
## UPDATE, RELETE, etc.)  SELECT's (and SELECT-lie) statements produce
## "dbResultSet" objects, which extend dbResult.

## Class: dbResultSet
fetch <- function(res, n, ...)
{
  UseMethod("fetch")
}
setDataMappings <- function(res, ...)
{
  UseMethod("setDataMappings")
}
close.resultSet <- function(con, ...)
{
  stop("close method for dbResultSet objects need to be written")
}

## Need to elevate the current load() to the load.default
if(!exists("load.default")){
  if(exists("load", mode = "function", where = match("package:base",
search())))
    load.default <- get("load", mode = "function", 
                                 pos = match("package:base", search()))
  else
    load.default <- function(file, ...) stop("method must be overriden")
}

## Need to elevate the current getConnection to a default method,
## and define getConnection to be a generic

if(!exists("getConnection.default")){
  if(exists("getConnection", mode="function", where=match("package:base",search())))
    getConnection.default <- get("getConnection", mode = "function",
                                 pos=match("package:base", search()))
  else
    getConnection.default <- function(what, ...) 
      stop("method must be overriden")
}
if(!usingR(1,2.1)){
  close <- function(con, ...) UseMethod("close")
}
getConnection <- function(what, ...)
{
  UseMethod("getConnection")
}
getFields <- function(res, ...)
{
  UseMethod("getFields")
}
getStatement <- function(res, ...)
{
  UseMethod("getStatement")
}
getRowsAffected <- function(res, ...)
{
  UseMethod("getRowsAffected")
}
getRowCount <- function(res, ...)
{
  UseMethod("getRowCount")
}
getNullOk <- function(res, ...)
{
  UseMethod("getNullOk")
}
hasCompleted <- function(res, ...)
{
  UseMethod("hasCompleted")
}
## these next 2 are meant to be used with tables (not general purpose
## result sets) thru connections
getNumRows <- function(con, name, ...)
{
   UseMethod("getNumRows")
}
getNumCols <- function(con, name, ...)
{
   UseMethod("getNumCols")
}
getNumCols.default <- function(con, name, ...)
{
   nrow(getFields(con, name))
}
## (I don't know how to efficiently and portably get num of rows of a table)

## Meta-data: 
## The approach in the current implementation is to have .Call()
##  functions return named lists with all the known features for
## the various objects (dbManager, dbConnection, dbResultSet, 
## etc.) and then each meta-data function (e.g., getVersion) extract 
## the appropriate field from the list.  Of course, there are meta-data
## elements that need to access to DBMS data dictionaries (e.g., list
## of databases, priviledges, etc) so they better be implemented using 
## the SQL inteface itself, say thru quickSQL.
## 
## It may be possible to get some of the following meta-data from the
## dbManager object iteslf, or it may be necessary to get it from a
## dbConnection because the dbManager does not talk itself to the
## actual DBMS.  The implementation will be driver-specific.  
##
## TODO: what to do about permissions? privileges? users? Some 
## databses, e.g., mSQL, do not support multiple users.  Can we get 
## away without these?  The basis for defining the following meta-data 
## is to provide the basics for writing methods for attach(db) and 
## related methods (objects, exist, assign, remove) so that we can even
## abstract from SQL and the RS-Database interface itself.

getInfo <- function(obj, ...)
{
  UseMethod("getInfo")
}
describe <- function(obj, verbose = F, ...)
{
  UseMethod("describe")
}
getVersion <- function(obj, ...)
{
  UseMethod("getVersion")
}
getCurrentDatabase <- function(obj, ...)
{
  UseMethod("getCurrentDatabase")
}
getDatabases <- function(obj, ...)
{
  UseMethod("getDatabases")
}
getTables <- function(obj, dbname, row.names, ...) 
{
  UseMethod("getTables")
}
getTableFields <- function(res, table, dbname, ...)
{
  UseMethod("getTableFields")
}
getTableIndices <- function(res, table, dbname, ...) 
{
  UseMethod("getTableIndices")
}

## Class: dbObjectId
##
## This helper class is *not* part of the database interface definition,
## but it is extended by both the Oracle and MySQL implementations to
## MySQLObject and OracleObject to allow us to conviniently implement 
## all database foreign objects methods (i.e., methods for show(), 
## print() format() the dbManger, dbConnection, dbResultSet, etc.) 
## A dbObjectId is an  identifier into an actual remote database objects.  
## This class and its derived classes <driver-manager>Object need to 
## be VIRTUAL to avoid coercion (green book, p.293) during method dispatching.

##setClass("dbObjectId", representation(Id = "integer", VIRTUAL))

new.dbObjectId <- function(Id, ...)
{
  new("dbObjectId", Id = Id)
}
## Coercion: the trick as(dbObject, "integer") is very useful
## (in R this needs to be define for each of the "mixin" classes -- Grr!)
as.integer.dbObjectId <- 
function(object)
{
  as.integer(object$Id)
}

"isIdCurrent" <- 
function(obj)
## verify that obj refers to a currently open/loaded database
{ 
  obj <- as(obj, "integer")
  .Call("RS_DBI_validHandle", obj)
}

"format.dbObjectId" <- 
function(x, ...)
{
  id <- as(x, "integer")
  paste("(", paste(id, collapse=","), ")", sep="")
}
"print.dbObjectId" <- 
function(x, ...)
{
  str <- paste(class(x), " id = ", format(x), sep="")
  if(isIdCurrent(x))
    cat(str, "\n")
  else
    cat("Expired", str, "\n")
  invisible(NULL)
}

## These are convenience functions that mimic S database access methods 
## get(), assign(), exists(), and remove().

"getTable" <-  function(con, name, ...)
{
  UseMethod("getTable")
}

"getTable.dbConnection" <- 
function(con, name, row.names = "row.names", check.names = T, ...)
## Should we also allow row.names to be a character vector (as in read.table)?
## is it "correct" to set the row.names of output data.frame?
## Use NULL, "", or 0 as row.names to prevent using any field as row.names.
{
  out <- quickSQL(con, paste("SELECT * from", name))
  if(check.names)
     names(out) <- make.names(names(out), unique = T)
  ## should we set the row.names of the output data.frame?
  nms <- names(out)
  j <- switch(mode(row.names),
         "character" = if(row.names=="") 0 else
                       match(tolower(row.names), tolower(nms), 
                             nomatch = if(missing(row.names)) 0 else -1),
         "numeric" = row.names,
         "NULL" = 0,
         0)
  if(j==0) 
     return(out)
  if(j<0 || j>ncol(out)){
     warning("row.names not set on output data.frame (non-existing field)")
     return(out)
  }
  rnms <- as.character(out[,j])
  if(all(!duplicated(rnms))){
      out <- out[,-j, drop = F]
      row.names(out) <- rnms
  } else warning("row.names not set on output (duplicate elements in field)")
  out
} 

"existsTable" <- function(con, name, ...)
{
  UseMethod("existsTable")
}
"existsTable.dbConnection" <- function(con, name, ...)
{
    ## name is an SQL (not an R/S!) identifier.
    match(name, getTables(con), nomatch = 0) > 0
}
"removeTable" <- function(con, name, ...)
{
  UseMethod("removeTable")
}
"removeTable.dbConnection" <- function(con, name, ...)
{
  if(existsTable(con, name, ...)){
    rc <- try(quickSQL(con, paste("DROP TABLE", name)))
    !inherits(rc, "Error")
  } 
  else  FALSE
}

"assignTable" <- function(con, name, value, row.names, ...)
{
  UseMethod("assignTable")
}

## The following generic returns the closest data type capable 
## of representing an R/S object in a DBMS.  
## TODO:  Lots! We should have a base SQL92 method that individual
## drivers extend?  Currently there is no default.  Should
## we also allow data type mapping from SQL -> R/S?

"SQLDataType" <- function(mgr, obj, ...)
{
  UseMethod("SQLDataType")
}
SQLDataType.default <- function(mgr, obj, ...)
{
   ## should we supply an SQL89/SQL92 default implementation?
   stop("must be implemented by a specific driver")
}
"make.SQL.names" <- 
function(snames, unique = T, allow.keywords = T)
## produce legal SQL identifiers from strings in a character vector
## unique, in this function, means unique regardless of lower/upper case
{
   makeUnique <- function(x, sep="_"){
     out <- x
     lc <- make.names(tolower(x), unique=F)
     i <- duplicated(lc)
     lc <- make.names(lc, unique = T)
     out[i] <- paste(out[i], substring(lc[i], first=nchar(out[i])+1), sep=sep)
     out
   }
   snames  <- make.names(snames, unique=F)
   if(unique) 
     snames <- makeUnique(snames)
   if(!allow.keywords){
     snames <- makeUnique(c(.SQLKeywords, snames))
     snames <- snames[-seq(along = .SQLKeywords)]
   } 
   .Call("RS_DBI_makeSQLNames", snames)
}

"isSQLKeyword" <-
function(x, keywords = .SQLKeywords, case = c("lower", "upper", "any")[3])
{
   n <- pmatch(case, c("lower", "upper", "any"), nomatch=0)
   if(n==0)
     stop('case must be one of "lower", "upper", or "any"')
   kw <- switch(c("lower", "upper", "any")[n],
           lower = tolower(keywords),
           upper = toupper(keywords),
           any = toupper(keywords))
   if(n==3)
     x <- toupper(x)
   match(x, keywords, nomatch=0) > 0
}
## SQL ANSI 92 (plus ISO's) keywords --- all 220 of them!
## (See pp. 22 and 23 in X/Open SQL and RDA, 1994, isbn 1-872630-68-8)
".SQLKeywords" <- 
c("ABSOLUTE", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS",
  "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN",
  "BIT", "BIT_LENGTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", 
  "CATALOG", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH",
  "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", 
  "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", 
  "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CURRENT",
  "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TYPE", "CURSOR", "DATE",
  "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", 
  "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR",
  "DIAGNOSTICS", "DICONNECT", "DICTIONATRY", "DISPLACEMENT", "DISTINCT",
  "DOMAIN", "DOUBLE", "DROP", "ELSE", "END", "END-EXEC", "ESCAPE", 
  "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", 
  "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", 
  "FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", "GRANT", 
  "GROUP", "HAVING", "HOUR", "IDENTITY", "IGNORE", "IMMEDIATE", "IN",
  "INCLUDE", "INDEX", "INDICATOR", "INITIALLY", "INNER", "INPUT", 
  "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL",
  "INTO", "IS", "ISOLATION", "JOIN", "KEY", "LANGUAGE", "LAST", "LEFT",
  "LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE",
  "MODULE", "MONTH", "NAMES", "NATIONAL", "NCHAR", "NEXT", "NOT", "NULL",
  "NULLIF", "NUMERIC", "OCTECT_LENGTH", "OF", "OFF", "ONLY", "OPEN",
  "OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS", "PARTIAL",
  "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR",
  "PRIVILEGES", "PROCEDURE", "PUBLIC", "READ", "REAL", "REFERENCES",
  "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA", "SCROLL",
  "SECOND", "SECTION", "SELECT", "SET", "SIZE", "SMALLINT", "SOME", "SQL",
  "SQLCA", "SQLCODE", "SQLERROR", "SQLSTATE", "SQLWARNING", "SUBSTRING",
  "SUM", "SYSTEM", "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP",
  "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRANSACTION", "TRANSLATE",
  "TRANSLATION", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER",
  "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING",
  "VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR",
  "ZONE"
)

## $Id$
##
## Copyright (C) 1999 The Omega Project for Statistical Computing.
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU Lesser General Public
## License as published by the Free Software Foundation; either
## version 2 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## Lesser General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public
## License along with this library; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA

## Class: dbManager

new.SQLiteObject <- function(Id)
{
   our <- as.integer(Id)
   class(out) <- c("SQLiteObject", "dbObjectId")
   out
}
new.SQLiteManager <- function(Id)
{
   out <- list(Id = Id)
   class(out) <- c("SQLiteManager", "dbManager", "SQLiteObject", "dbObjectId")
   out
}
"SQLite" <- "SQLiteManager" <- 
function(max.con=10, fetch.default.rec = 500, force.reload=F)
## create a SQLite database connection manager.  By default we allow
## up to "max.con" connections and single fetches of up to "fetch.default.rec"
## records.  These settings may be changed by re-loading the driver
## using the "force.reload" = T flag (note that this will close all 
## currently open connections).
## Returns an object of class "SQLiteManager".  
## Note: This class is a singleton.
{
  if(fetch.default.rec<=0)
    stop("default num of records per fetch must be positive")
  id <- load.SQLiteManager(max.con, fetch.default.rec, force.reload)
  new("SQLiteManager", Id = id)
}

as.integer.dbObjectId <- function(x, ...)
{
  x <- x$Id
  NextMethod("as.integer")
}
as.SQLiteManager <- function(object)
{
  new("SQLiteManager", Id = as(object, "integer")[1])
}
loadManager.SQLiteManager <- function(mgr, ...)
  load.SQLiteManager(...)

## Class: dbConnection
dbConnect.SQLiteManager <- function(mgr, ...)
{
  id <- newConnection.SQLiteManager(mgr, ...)
  new("SQLiteConnection",Id = id)
}
dbConnect.SQLiteConnection <- function(mgr, ...)
{
  con.id <- as(mgr, "integer")
  con <- .Call("RS_SQLite_cloneConnection", con.id)
  new("SQLiteConnection", Id = con)
}
dbConnect.default <- function(mgr, ...)
{
## dummy default (it only works for SQLite)  See the S4 for the
## real default method
  if(!is.character(mgr)) 
    stop("mgr must be a string with the driver name")
  id <- do.call(mgr, list())
  dbConnect(id, ...)
}
getConnections.SQLiteManager <- function(mgr, ...)
{
   getInfo(mgr, what = "connectionIds")
}
getManager.SQLiteConnection <- getManager.SQLiteResultSet <-
function(obj, ...)
{
   as(obj, "SQLiteManager")
}
quickSQL <- function(con, statement, ...)
{
  UseMethod("quickSQL")
}
getException.SQLiteConnection <- function(object)
{
  id <- as.integer(object)
  .Call("RS_SQLite_getException", id)
}
dbExec.SQLiteConnection <- 
dbExecStatement.SQLiteConnection <- 
function(con, statement, ...)
{
  if(!is.character(statement))
    stop("non character statement")
  execStatement.SQLiteConnection(con, statement, ...)
}


## Class: resultSet

new.SQLiteResultSet <- function(Id)
{
  out <- as.integer(Id)
  class(out) <- c("SQLiteResultSet", "dbResultSet", "SQLiteObject", "dbObjectId")
  out
}

getConnection.SQLiteConnection <-
getConnection.SQLiteResultSet <- 
function(object)
{
  new("SQLiteConnection", Id=as(object, "integer")[1:2])
}

getStatement.SQLiteResultSet <- function(object)
{
  getInfo.SQLiteResultSet(object, "statement")
}

## ???
getFields.SQLiteResultSet <- function(res)
{
  flds <- getInfo.SQLiteResultSet(res, "fieldDescription")[[1]]
  flds$Sclass <- .Call("RS_DBI_SclassNames", flds$Sclass, PACKAGE="RSQLite")
  flds$type <- .Call("RS_SQLite_getFieldTypeNames", flds$type,
                      PACKAGE="RSQLite")
  structure(flds, row.names = paste(seq(along=flds$type)), class="data.frame")
}

getRowsAffected.SQLiteResultSet <- function(object)
  getInfo.SQLiteResultSet(object, "rowsAffected")

getRowCount.SQLiteResultSet <- function(object)
  getInfo.SQLiteResultSet(object, "rowCount")

hasCompleted.SQLiteResultSet <- function(object)
  getInfo.SQLiteResultSet(object, "completed")

getException.SQLiteResultSet <- function(object)
{
  id <- as.integer(object)
  .Call("RS_SQLite_getException", id[1:2])
}

getCurrentDatabase.SQLiteConnection <- function(object, ...)
{
  getInfo(con, "dbname")
}

getDatabases.SQLiteConnection <- function(obj, ...)
{
   stop("not implemented")
}

getTables.SQLiteConnection <- function(object, dbname = stop("not implemented"), ...)
{
   quickSQL(object, 
       'select name from sqlite_master where type="table" order by name',
       ...)[,1]
}

getTableFields.SQLiteResultSet <- function(object, table, dbname, ...)
{
  getFields(object)
}

getTableFields.SQLiteConnection <- function(object, table, dbname, ...)
{
## SQLite does not provide this metadata, so we have to fake it ourselves
  if(!missing(dbname))
     stop("not implemented")
  if(length(getResultSets(con))>0){
     con2 <- dbConnect(con)
     on.exit(close(con2))
  } else con2 <- con
  rs <- dbExecStatement(con2, paste("select * from ", table), limit = 1)
  flds <- getFields(rs)
  dummy <- fetch(rs, n = 1)
  close(rs)
  flds
}

getTableIndices.SQLiteConnection <- function(obj, table, dbname, ...)
{
  stop("not yet implemented")
  if (missing(dbname))
    cmd <- paste("show index from", table)
  else cmd <- paste("show index from", table, "from", dbname)
  quickSQL(obj, cmd)
}

"assignTable.SQLiteConnection" <-
function(con, name, value, field.types, row.names = T, 
  overwrite=F, append=F, ...)
## TODO: This function should execute its sql as a single transaction,
## and allow converter functions.
## Create table "name" (must be an SQL identifier) and populate
## it with the values of the data.frame "value"
## BUG: In the unlikely event that value has a field called "row.names"
## we could inadvertently overwrite it (here the user should set row.names=F)
## (I'm reluctantly adding the code re: row.names -- I'm not 100% comfortable
## using data.frames as the basic data for relations.)
{
  if(overwrite && append)
    stop("overwrite and append cannot both be TRUE")
  if(!is.data.frame(value))
    value <- as.data.frame(value)
  if(row.names){
    value  <- cbind(row.names(value), value)  ## can't use row.names= here
    names(value)[1] <- "row.names" 
  }
  if(missing(field.types) || is.null(field.types)){
    ## the following mapping should be coming from some kind of table
    ## also, need to use converter functions (for dates, etc.)
    field.types <- sapply(value, SQLDataType, mgr = con)
  } 
  i <- match("row.names", names(field.types), nomatch=0)
  if(i>0) ## did we add a row.names value?  If so, it's a text field.
    field.types[i] <- SQLDataType(mgr=con, field.types$row.names)
  names(field.types) <- make.SQL.names(names(field.types), allow.keywords=F)

  ## Do we need to clone the connection (ie., if it is in use)?
  if(length(getResultSets(con))!=0){ 
    new.con <- dbConnect(con)              ## there's pending work, so clone
    on.exit(close(new.con))
  } 
  else {
    new.con <- con
  }

  if(existsTable(con,name)){
    if(overwrite){
      if(!removeTable(con, name)){
        warning(paste("table", name, "couldn't be overwritten"))
        return(F)
      }
    }
    else if(!append){
      warning(paste("table", name, "exists in database: aborting assignTable"))
      return(F)
    }
  } 
  if(!existsTable(con,name)){      ## need to re-test table for existance 
    ## need to create a new (empty) table
    sql1 <- paste("create table ", name, "\n(\n\t", sep="")
    sql2 <- paste(paste(names(field.types), field.types), collapse=",\n\t", 
                  sep="")
    sql3 <- "\n)\n"
    sql <- paste(sql1, sql2, sql3, sep="")
    rs <- try(dbExecStatement(new.con, sql))
    if(inherits(rs, ErrorClass)){
      warning("could not create table: aborting assignTable")
      return(F)
    } 
    else 
      close(rs)
  }

  fn <- tempfile("rsdbi")
  if(usingR())
    write.table(value, file = fn, quote = F, sep="\t", 
                na = .SQLite.NA.string, row.names=F, col.names=F, eol = '\n')
  else
    write.table(value, file = fn, quote.string = F, sep="\t", 
                na = .SQLite.NA.string, dimnames.write=F, end.of.row = '\n')
  on.exit(unlink(fn), add = T)
  sql4 <- paste("COPY '", name,"' FROM '", fn, "'", sep="")
  rs <- try(dbExecStatement(new.con, sql4))
  if(inherits(rs, ErrorClass)){
    warning("could not load data into table")
    return(F)
  } 
  else 
    close(rs)
  TRUE
}

SQLDataType.SQLiteConnection <-
SQLDataType.SQLiteManager <- 
function(obj, ...)
## find a suitable SQL data type for the R/S object obj
## TODO: Lots and lots!! (this is a very rough first draft)
## need to register converters, abstract out SQLite and generalize 
## to Oracle, Informix, etc.  Perhaps this should be table-driven.
{
  rs.class <- data.class(obj)
  rs.mode <- storage.mode(obj)
  if(rs.class=="numeric"){
    sql.type <- if(rs.mode=="integer") "bigint" else  "double"
  } 
  else {
    sql.type <- switch(rs.class,
                  character = "text",
                  logical = "tinyint",
                  factor = "text",	## up to 65535 characters
                  ordered = "text",
                  "text")
  }
  sql.type
}
## $Id$
##
## Copyright (C) 1999 The Omega Project for Statistical Computing.
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU Lesser General Public
## License as published by the Free Software Foundation; either
## version 2 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## Lesser General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public
## License along with this library; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
##

## RS-SQLite Support functions.  These functions are named
## following S3/R convention "<method>.<class>" (e.g., close.SQLiteconnection)
## to allow easy porting to R and S3(?).  Also, we tried to minimize
## S4 specific construct as much as possible (there're still a few
## S4 idioms left, but few).
##

.SQLite.NA.string <- "\\N"  ## on input, MySQL/PostgrSQL/SQLite(?) interprets \N as NULL (NA)

if(usingR()){
  format.SQLiteManager <- format.SQLiteConnection <-
  format.SQLiteResultSet <- function(x){
    format.dbObjectId(x)
  }
  print.SQLiteManager <- print.SQLiteConnection <-
  print.SQLiteResultSet <- function(x, ...){
    print.dbObjectId(x, ...)
  }
  as.integer.SQLiteManager <- as.integer.SQLiteConnection <-
  as.integer.SQLiteResultSet <- function(object){
     as.integer(object$Id)
  }
  as.SQLiteManager <- function(obj){
     new("SQLiteManager", Id =as(obj,"integer")[1])
  }
  as.SQLiteConnection <- function(obj){
    new("SQLiteConnection", Id = as(obj, "integer")[1:2])
  } 
  as.SQLiteResultSet <- function(obj){
    new("SQLiteResultSet", Id = as(obj, "integer")[1:3])
  }

  ## these, again, are needed only because virtual classes (dbObject)
  ## are not available in R
  getTable.SQLiteConnection <- function(con, name, ...){
    getTable.dbConnection(con, name, ...)
  }
  existsTable.SQLiteConnection <- function(con, name, ...){
    existsTable.dbConnection(con, name, ...)
  }
  removeTable.SQLiteConnection <- function(con, name, ...){
    removeTable.dbConnection(con, name, ...)
  }
}
"load.SQLiteManager" <- 
function(max.con = 16, fetch.default.rec = 500, force.reload=F)
## return a manager id
{
  config.params <- as.integer(c(max.con, fetch.default.rec))
  force <- as.logical(force.reload)
  .Call("RS_SQLite_init", config.params, force, PACKAGE = "RSQLite")
}

"describe.SQLiteManager" <-
function(obj, verbose = F, ...)
## Print out nicely a brief description of the connection Manager
{
  info <- getInfo.SQLiteManager(obj)
  show(obj)
  cat("  Driver name: ", info$drvName, "\n")
  cat("  Max  connections:", info$length, "\n")
  cat("  Conn. processed:", info$counter, "\n")
  cat("  Default records per fetch:", info$"fetch_default_rec", "\n")
  if(verbose){
    cat("  SQLite client version: ", info$clientVersion, "\n")
    cat("  RS-DBI version: ", "0.2", "\n")
  }
  cat("  Open connections:", info$"num_con", "\n")
  if(verbose && !is.null(info$connectionIds)){
    for(i in seq(along = info$connectionIds)){
      cat("   ", i, " ")
      show(info$connectionIds[[i]])
    }
  }
  invisible(NULL)
}

"unload.SQLiteManager"<- 
function(mgr, ...)
{
  mgrId <- as(mgr, "integer")
  .Call("RS_SQLite_closeManager", mgrId, PACKAGE = "RSQLite")
}

"getInfo.SQLiteManager" <- 
function(obj, what="", ...)
{
  mgrId <- as(obj, "integer")[1]
  info <- .Call("RS_SQLite_managerInfo", mgrId, PACKAGE = "RSQLite")  
  mgrId <- info$managerId
  ## replace mgr/connection id w. actual mgr/connection objects
  conObjs <- vector("list", length = info$"num_con")
  ids <- info$connectionIds
  for(i in seq(along = ids))
    conObjs[[i]] <- new("SQLiteConnection", Id = c(mgrId, ids[i]))
  info$connectionIds <- conObjs
  info$managerId <- new("SQLiteManager", Id = mgrId)
  if(length(what)==1 && what=="")
    return(info)
  info <- info[what]
  if(length(info)==1)
    info[[1]]
  else
    info
}

"getVersion.SQLiteManager" <- 
function(mgr)
{
  ## TODO: the DBI version number should be coming from the manager obj
  ## TODO: should also report the R/S SQLite driver version 
  list("DBI" = "0.2",
       "SQLite (client) library" = getInfo(mgr, what="clientVersion"))
}
"getVersion.SQLiteConnection" <-
function(con)
{
   getInfo(con, what = c("serverVersion", "protocolVersion"))
}
## note that dbname may be a database name, an empty string "", or NULL.
## The distinction between "" and NULL is that "" is interpreted by 
## the SQLite API as the default database (SQLite config specific)
## while NULL means "no database".
"newConnection.SQLiteManager"<- 
function(mgr, dbname = "", mode=0)
{
  con.params <- as.character(c(dbname, mode))
  mgrId <- as(mgr, "integer")
  .Call("RS_SQLite_newConnection", mgrId, con.params, PACKAGE ="RSQLite")
}

## functions/methods not implementable
"commit.SQLiteConnection" <- 
"rollback.SQLiteConnection" <- 
function(con) 
{
  warning("SQLite does not support transactions")
}

"describe.SQLiteConnection" <- 
function(obj, verbose = F, ...)
{
  info <- getInfo(obj)
  show(obj)
  cat("  User:", info$user, "\n")
  cat("  Host:", info$host, "\n")
  cat("  Dbname:", info$dbname, "\n")
  cat("  Connection type:", info$conType, "\n")
  if(verbose){
    cat("  SQLite server version: ", info$serverVersion, "\n")
    cat("  SQLite client version: ", 
	getInfo(as(obj, "SQLiteManager"),what="clientVersion"), "\n")
    cat("  SQLite protocol version: ", info$protocolVersion, "\n")
    cat("  SQLite server thread id: ", info$threadId, "\n")
  }
  if(length(info$rsId)>0){
    for(i in seq(along = info$rsId)){
      cat("   ", i, " ")
      show(info$rsId[[i]])
    }
  } else 
    cat("  No resultSet available\n")
  invisible(NULL)
}

"close.SQLiteConnection" <- 
function(con, ...)
{
  conId <- as(con, "integer")
  .Call("RS_SQLite_closeConnection", conId, PACKAGE = "RSQLite")
}

"getInfo.SQLiteConnection" <-
function(obj, what="", ...)
{
  if(!isIdCurrent(obj))
    stop(paste("expired", class(obj), deparse(substitute(obj))))
  id <- as(obj, "integer")
  info <- .Call("RS_SQLite_connectionInfo", id, PACKAGE = "RSQLite")
  if(length(info$rsId)){
    rsId <- vector("list", length = length(info$rsId))
    for(i in seq(along = info$rsId))
      rsId[[i]] <- new("SQLiteResultSet", Id = c(id, info$rsId[i]))
    info$rsId <- rsId
  }
  else
    info$rsId <- NULL
  if(length(what)==1 && what=="")
    return(info)
  info <- info[what]
  if(length(info)==1)
    info[[1]]
  else
    info
}
       
"execStatement.SQLiteConnection" <- 
function(con, statement, limit = -1)
## submits the sql statement to SQLite and creates a
## dbResult object if the SQL operation does not produce
## output, otherwise it produces a resultSet that can
## be used for fetching rows.
## limit specifies how many rows we actually put in the
## resultSet
{
  conId <- as(con, "integer")
  statement <- as(statement, "character")
  limit <- as(limit, "integer")
  rsId <- .Call("RS_SQLite_exec", 
                conId, statement, limit, 
                PACKAGE = "RSQLite")
#  out <- new("SQLitedbResult", Id = rsId)
#  if(getInfo(out, what="isSelect")
#    out <- new("SQLiteResultSet", Id = rsId)
#  out
  out <- new("SQLiteResultSet", Id = rsId)
  out
}

getResultSets.SQLiteConnection <-
function(con)
{
  getInfo(con, what = "rsId")
}
## helper function: it exec's *and* retrieves a statement. It should
## be named somehting else.
quickSQL.SQLiteConnection <- function(con, statement)
{
  nr <- length(getResultSets(con))
  if(nr>0){                     ## are there resultSets pending on con?
    new.con <- dbConnect(con)   ## yep, create a clone connection
    on.exit(close(new.con))
    rs <- dbExecStatement(new.con, statement)
  } else rs <- dbExecStatement(con, statement)
  if(hasCompleted(rs)){
    close(rs)            ## no records to fetch, we're done
    invisible()
    return(NULL)
  }
  res <- fetch(rs, n = -1)
  if(hasCompleted(rs))
    close(rs)
  else 
    warning("pending rows")
  res
}

## Experimental dbApply (should it be seqApply?)
dbApply <- function(rs, ...)
{
   UseMethod("dbApply")
}

"dbApply.SQLiteResultSet" <- 
function(rs, INDEX, FUN = stop("must specify FUN"), 
         begin = NULL, 
         group.begin =  NULL, 
         new.record = NULL, 
         end = NULL, 
         batchSize = 100, maxBatch = 1e6, 
         ..., simplify = T)
{
   stop("not yet implemented")
}

"fetch.SQLiteResultSet" <- 
function(res, n=0, ...)   
## Fetch at most n records from the opened resultSet (n = -1 meanSQLite
## all records, n=0 means extract as many as "default_fetch_rec",
## as defined by SQLiteManager (see describe(mgr, T)).
## The returned object is a data.frame. 
## Note: The method hasCompleted() on the resultSet tells you whether
## or not there are pending records to be fetched. See also the methods
## describe(), getFieldDescrition(), getRowCount(), getAffectedRows(),
## getDBConnection(), getException().
## 
## TODO: Make sure we don't exhaust all the memory, or generate
## an object whose size exceeds option("object.size").  Also,
## are we sure we want to return a data.frame?
{    

  type.convert <- function(x, na.strings="NA", as.is=TRUE, dec="."){
     .Internal(type.convert(x, na.strings, as.is, dec))
  }
  n <- as(n, "integer")
  rsId <- as(res, "integer")
  rel <- .Call("RS_SQLite_fetch", rsId, nrec = n, PACKAGE = "RSQLite")
  if(length(rel)==0 || length(rel[[1]])==0) 
    return(NULL)
  for(j in seq(along = rel))
      rel[[j]] <- type.convert(rel[[j]], ...)
  ## create running row index as of previous fetch (if any)
  cnt <- getRowCount(res)
  nrec <- length(rel[[1]])
  indx <- seq(from = cnt - nrec + 1, length = nrec)
  attr(rel, "row.names") <- as.character(indx)
  oldClass(rel) <- "data.frame"
  rel
}

## Note that originally we had only resultSet both for SELECTs
## and INSERTS, ...  Later on we created a base class dbResult
## for non-Select SQL and a derived class resultSet for SELECTS.

"getInfo.SQLiteResultSet" <- 
#"getInfo.SQLitedbResult" <- 
function(obj, what = "", ...)
{
  if(!isIdCurrent(obj))
    stop(paste("expired", class(obj), deparse(substitute(obj))))
   id <- as(obj, "integer")
   info <- .Call("RS_SQLite_resultSetInfo", id, PACKAGE = "RSQLite")
   if(length(what)==1 && what=="")
     return(info)
   info <- info[what]
   if(length(info)==1)
     info[[1]]
   else
     info
}

if(F){
  ##"describe.SQLiteResultSet" <- 
  "describe.SQLitedbResult" <- 
    function(obj, verbose = F, ...)
      {
	if(!isIdCurrent(obj)){
	  show(obj)
	  invisible(return(NULL))
	}
	show(obj)
	cat("  Statement:", getStatement(obj), "\n")
	cat("  Has completed?", if(hasCompleted(obj)) "yes" else "no", "\n")
	cat("  Affected rows:", getRowsAffected(obj), "\n")
	invisible(NULL)
      }
}
"describe.SQLiteResultSet" <- 
function(obj, verbose = F, ...)
{

  if(!isIdCurrent(obj)){
    show(obj)
    invisible(return(NULL))
  }
  show(obj)
  cat("  Statement:", getStatement(obj), "\n")
  cat("  Has completed?", if(hasCompleted(obj)) "yes" else "no", "\n")
  cat("  Affected rows:", getRowsAffected(obj), "\n")
  cat("  Rows fetched:", getRowCount(obj), "\n")
  flds <- getFields(obj)
  if(verbose && !is.null(flds)){
    cat("  Fields:\n")  
    out <- print(getFields(obj))
  }
  invisible(NULL)
}

"close.SQLiteResultSet" <- 
function(con, ...)
{
  rsId <- as(con, "integer")
  .Call("RS_SQLite_closeResultSet", rsId, PACKAGE = "RSQLite")
}
.conflicts.OK <- TRUE
.First.lib <- function(lib, pkg) {
  library.dynam("RSQLite", pkg, lib)
}
