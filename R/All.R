## $Id: zzz.R,v 1.6 2005/04/06 02:11:13 dj Exp dj $
".conflicts.OK" <- TRUE

## these are needed while source'ing the package (prior
## to library.dyname).
library(methods, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

".First.lib" <- 
function(lib, pkg)
{
   ## need to dyn.load sqlite.dll before we attempt to load
   ## RSQLite.dll  -- there's got to be a better way...

   if(.Platform$OS.type=="windows"){
      if(!is.loaded(symbol.C("sqlite3_libversion")))
         dyn.load(system.file("libs", "sqlite3.dll", package = "RSQLite"))
   }

   library.dynam("RSQLite", pkg, lib)
   library(methods, warn.conflicts = FALSE)
   library(DBI, warn.conflicts = FALSE)
}
##
## $Id: S4R.R,v 1.2 2002/08/27 13:44:27 dj Exp $
##
## S4/Splus/R compatibility 

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
##
## $Id: dbObjectId.R,v 1.1 2002/08/27 13:42:04 dj Exp $
## 
## Copyright (C) 1999-2002 The Omega Project for Statistical Computing.
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

## Class: dbObjectId
##
## This mixin helper class is NOT part of the database interface definition,
## but it is extended by the Oracle, MySQL, and SQLite implementations to
## MySQLObject and OracleObject to allow us to conviniently (and portably) 
## implement all database foreign objects methods (i.e., methods for show(), 
## print() format() the dbManger, dbConnection, dbResultSet, etc.) 
## A dbObjectId is an  identifier into an actual remote database objects.  
## This class and its derived classes <driver-manager>Object need to 
## be VIRTUAL to avoid coercion (green book, p.293) during method dispatching.
##
## TODO: Convert the Id slot to be an external object (as per Luke Tierney's
## implementation), even at the expense of S-plus compatibility?

setClass("dbObjectId", representation(Id = "integer", "VIRTUAL"))

## coercion methods 
setAs("dbObjectId", "integer", 
   def = function(from) as(slot(from,"Id"), "integer")
)
setAs("dbObjectId", "numeric",
   def = function(from) as(slot(from, "Id"), "integer")
)
setAs("dbObjectId", "character",
   def = function(from) as(slot(from, "Id"), "character")
)   

## formating, showing, printing,...
setMethod("format", "dbObjectId", 
   def = function(x, ...) {
      paste("(", paste(as(x, "integer"), collapse=","), ")", sep="")
   },
   valueClass = "character"
)

setMethod("show", "dbObjectId", def = function(object) print(object))

setMethod("print", "dbObjectId",
   def = function(x, ...){
      expired <- if(isIdCurrent(x)) "" else "Expired "
      str <- paste("<", expired, class(x), ":", format(x), ">", sep="")
      cat(str, "\n")
      invisible(NULL)
   }
)

"isIdCurrent" <- 
function(obj)
## verify that obj refers to a currently open/loaded database
{ 
   obj <- as(obj, "integer")
   .Call("RS_DBI_validHandle", obj, PACKAGE = .SQLitePkgName)
}
##
## $Id: SQLite.R,v 1.3 2003/06/17 15:00:52 dj Exp dj $
##
## Copyright (C) 1999-2003 The Omega Project for Statistical Computing.
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

##
## Constants
##

.SQLitePkgName <- "RSQLite"
.SQLitePkgRCS <- "$Id: SQLite.R,v 1.3 2003/06/17 15:00:52 dj Exp dj $"
.SQLite.NA.string <- "\\N"  ## on input SQLite interprets \N as NULL (NA)

setOldClass("data.frame")   ## to avoid warnings in setMethod's valueClass arg

##
## Class: SQLiteDriver
##

setClass("SQLiteObject", representation("DBIObject", "dbObjectId", "VIRTUAL"))
setClass("SQLiteDriver", representation("DBIDriver", "SQLiteObject"))

"SQLite" <- 
function(max.con=16, fetch.default.rec = 500, force.reload=FALSE)
{
   sqliteInitDriver(max.con, fetch.default.rec, force.reload)
}

setMethod("dbUnloadDriver", "SQLiteDriver",
   def = function(drv, ...) sqliteCloseDriver(drv, ...),
   valueClass = "logical"
)
setMethod("dbGetInfo", "SQLiteDriver",
   def = function(dbObj, ...) sqliteDriverInfo(dbObj, ...),
   valueClass = "list"
)
setMethod("dbListConnections", "SQLiteDriver",
   def = function(drv, ...) {
      cons <- dbGetInfo(drv, what = "connectionIds")[[1]]
      if(!is.null(cons)) cons else list()
   },
   valueClass = "list"
)
setMethod("summary", "SQLiteDriver",
   def = function(object, ...) sqliteDescribeDriver(object, ...)
)

##
## Class: SQLiteConnection
##

setClass("SQLiteConnection", representation("DBIConnection", "SQLiteObject"))

setAs("SQLiteConnection", "SQLiteDriver", 
   def = function(from) new("SQLiteDriver", Id = as(from,"integer")[1])
)
setMethod("dbConnect", "SQLiteDriver", 
   def = function(drv, ...) sqliteNewConnection(drv, ...),
   valueClass = "SQLiteConnection"
)
setMethod("dbConnect", "character",
   def = function(drv, ...){
      d <- dbDriver(drv)
      dbConnect(d, ...)
   },
   valueClass = "SQLiteConnection"
)
setMethod("dbConnect", "SQLiteConnection",
   def = function(drv, ...){
      con.id <- as(drv, "integer")
      new.id <- .Call("RS_SQLite_cloneConnection", con.id, PACKAGE = "RSQLite")
      new("SQLiteConnection", Id = new.id)
   },
   valueClass = "SQLiteConnection"
)
setMethod("dbDisconnect", "SQLiteConnection",
   def = function(conn, ...) sqliteCloseConnection(conn, ...),
   valueClass = "logical"
)
setMethod("dbGetInfo", "SQLiteConnection",
   def = function(dbObj, ...) sqliteConnectionInfo(dbObj, ...),
   valueClass = "list"
)
setMethod("dbGetException", "SQLiteConnection",
   def = function(conn, ...){
      id <- as(conn, "integer")
      .Call("RS_SQLite_getException", id, PACKAGE = "RSQLite")
   },
   valueClass = "list"    ## TODO: should return a SQLiteException
)
setMethod("dbSendQuery", 
   sig = signature(conn = "SQLiteConnection", statement = "character"),
   def = function(conn, statement, ...){
      sqliteExecStatement(conn, statement, ...)
   },
   valueClass = "SQLiteResult"
)
setMethod("dbGetQuery", 
   sig = signature(conn = "SQLiteConnection", statement = "character"),
   def = function(conn, statement, ...){
      sqliteQuickSQL(conn, statement, ...)
   },
)
setMethod("summary", "SQLiteConnection",
   def = function(object, ...) sqliteDescribeConnection(object, ...)
)

setMethod("dbCallProc", "SQLiteConnection",
   def = function(conn, ...) .NotYetImplemented()
)

##
## Convenience methods
##

setMethod("dbExistsTable", 
   sig = signature(conn = "SQLiteConnection", name = "character"),
   def = function(conn, name, ...){
      lst <- dbListTables(conn)
      match(tolower(name), tolower(lst), nomatch = 0) > 0
   },
   valueClass = "logical"
)
setMethod("dbReadTable", 
   sig = signature(conn = "SQLiteConnection", name = "character"),
   def = function(conn, name, ...) sqliteReadTable(conn, name, ...),
   valueClass = "data.frame"
)
setMethod("dbWriteTable", 
   sig = signature(conn = "SQLiteConnection", name = "character",
                   value="data.frame"),
   def = function(conn, name, value, ...)
      sqliteWriteTable(conn, name, value, ...),
   valueClass = "logical"
)
setMethod("dbRemoveTable", 
   sig = signature(conn = "SQLiteConnection", name = "character"),
   def = function(conn, name, ...){
      rc <- try(dbGetQuery(conn, paste("drop table", name)))
      !inherits(rc, ErrorClass)
   },
   valueClass = "logical"
)
setMethod("dbListResults", "SQLiteConnection",
   def = function(conn, ...) {
      rs <- dbGetInfo(conn, "rsId")[[1]]
      if(length(rs)>0) rs else list()
   },
   valueClass = "list"
)

##
## Class: SQLiteResult
##

setClass("SQLiteResult", representation("DBIResult", "SQLiteObject"))

setAs("SQLiteResult", "SQLiteConnection", 
   def = function(from) new("SQLiteConnection", Id = as(from,"integer")[1:2])
)
setMethod("fetch", "SQLiteResult",
   def = function(res, n = 0, ...) sqliteFetch(res, n = n, ...),
   valueClass = "data.frame"
)
setMethod("dbClearResult", "SQLiteResult",
   def = function(res, ...) sqliteCloseResult(res, ...),
   valueClass = "logical"
)
setMethod("dbGetInfo", "SQLiteResult",
   def = function(dbObj, ...) sqliteResultInfo(dbObj, ...),
   valueClass = "list"
)
setMethod("dbGetStatement", "SQLiteResult",
   def = function(res, ...) dbGetInfo(res, "statement")[[1]],
   valueClass = "character"
)
setMethod("dbColumnInfo", "SQLiteResult",
   def = function(res, ...){
      out <- dbGetInfo(res, "fields")[[1]]
      if(!is.null(out)) out else data.frame(out)
   },
   valueClass = "data.frame"
)
setMethod("dbGetRowsAffected", "SQLiteResult",
   def = function(res, ...) dbGetInfo(res, "rowsAffected")[[1]],
   valueClass = "integer"
)
setMethod("dbGetRowCount", "SQLiteResult",
   def = function(res, ...) dbGetInfo(res, "rowCount")[[1]],
   valueClass = "integer"
)
setMethod("dbHasCompleted", "SQLiteResult",
   def = function(res, ...){
      out <- dbGetInfo(res, "completed")[[1]]
      if(out<0) 
         NA
      else ifelse(out==0, FALSE, TRUE)
   },
   valueClass = "logical"
)
setMethod("dbGetException", "SQLiteConnection",
   def = function(conn, ...){
     id <- as.integer(conn)
     .Call("RS_SQLite_getException", id[1:2], PACKAGE = "RSQLite")
   },
   valueClass = "list"
)

setMethod("dbListTables", "SQLiteConnection", 
   def = function(conn, ...){
      out <- dbGetQuery(conn, 
         "select name from sqlite_master where type='table' or type='view' order by name",
         ...)[,1]
      if(!is.null(out)) out else character()
   },
   valueClass = "character"
)
setMethod("dbListFields", 
   sig = signature(conn = "SQLiteConnection", name = "character"),
   def = function(conn, name, ...) sqliteTableFields(conn, name, ...),
   valueClass = "character"
)

setMethod("summary", "SQLiteResult",
   def = function(object, ...) sqliteDescribeResult(object, ...)
)

##
## Utilities
##

setMethod("dbDataType", "SQLiteObject",
   def = function(dbObj, obj, ...) sqliteDataType(obj, ...),
   valueClass = "character"
)


setMethod("make.db.names", 
   signature(dbObj="SQLiteObject", snames = "character"),
   def = function(dbObj, snames, ...){
      make.db.names.default(snames, ...)
   },
   valueClass = "character"
)
      
setMethod("SQLKeywords", "SQLiteObject",
   def = function(dbObj, ...) .SQL92Keywords,
   valueClass = "character"
)

setMethod("isSQLKeyword",
   signature(dbObj="SQLiteObject", name="character"),
   def = function(dbObj, name, ...){
        isSQLKeyword.default(name, keywords = .SQL92Keywords)
   },
   valueClass = "character"
)
##
## $Id: SQLiteSupport.R,v 1.3 2005/04/06 02:10:24 dj Exp dj $
##
## Copyright (C) 1999-2002 The Omega Project for Statistical Computing.
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

"sqliteInitDriver" <- 
function(max.con = 16, fetch.default.rec = 500, force.reload=FALSE)
## return a manager id
{
  config.params <- as.integer(c(max.con, fetch.default.rec))
  force <- as.logical(force.reload)
  id <- .Call("RS_SQLite_init", config.params, force, PACKAGE = "RSQLite")
  new("SQLiteDriver", Id = id)
}

"sqliteCloseDriver" <- 
function(drv, ...)
{
  drvId <- as(drv, "integer")
  .Call("RS_SQLite_closeManager", drvId, PACKAGE = "RSQLite")
}

"sqliteDescribeDriver" <-
function(obj, verbose = FALSE, ...)
## Print out nicely a brief description of the connection Manager
{
  if(!isIdCurrent(obj)){
     show(obj)
     invisible(return(NULL))
  }
  info <- dbGetInfo(obj)
  show(obj)
  cat("  Driver name: ", info$drvName, "\n")
  cat("  Max  connections:", info$length, "\n")
  cat("  Conn. processed:", info$counter, "\n")
  cat("  Default records per fetch:", info$"fetch_default_rec", "\n")
  if(verbose){
    cat("  SQLite client version: ", info$clientVersion, "\n")
    cat("  DBI version: ", dbGetDBIVersion(), "\n")
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

"sqliteDriverInfo" <- 
function(obj, what="", ...)
{
  if(!isIdCurrent(obj))
    stop(paste("expired", class(obj)))
  drvId <- as(obj, "integer")[1]
  info <- .Call("RS_SQLite_managerInfo", drvId, PACKAGE = "RSQLite")  
  drvId <- info$managerId
  ## replace drv/connection id w. actual drv/connection objects
  conObjs <- vector("list", length = info$"num_con")
  ids <- info$connectionIds
  for(i in seq(along = ids))
    conObjs[[i]] <- new("SQLiteConnection", Id = c(drvId, ids[i]))
  info$connectionIds <- conObjs
  info$managerId <- new("SQLiteDriver", Id = drvId)
  if(!missing(what))
    info[what]
  else
    info
}

## note that dbname may be a database name, an empty string "", or NULL.
## The distinction between "" and NULL is that "" is interpreted by 
## the SQLite API as the default database (SQLite config specific)
## while NULL means "no database".
"sqliteNewConnection"<- 
function(drv, dbname = "", mode=0)
{
  con.params <- as.character(c(dbname, mode))
  drvId <- as(drv, "integer")
  conId <- .Call("RS_SQLite_newConnection", drvId, con.params, 
                 PACKAGE ="RSQLite")
  new("SQLiteConnection", Id = conId)
}

"sqliteDescribeConnection" <- 
function(obj, verbose = FALSE, ...)
{
  if(!isIdCurrent(obj)){
     show(obj)
     invisible(return(NULL))
  }
  info <- dbGetInfo(obj)
  show(obj)
  cat("  User:", info$user, "\n")
  cat("  Host:", info$host, "\n")
  cat("  Dbname:", info$dbname, "\n")
  cat("  Connection type:", info$conType, "\n")
  if(verbose){
    cat("  SQLite engine version: ", info$serverVersion, "\n")
    cat("  SQLite engine thread id: ", info$threadId, "\n")
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

"sqliteCloseConnection" <- 
function(con, ...)
{
  if(!isIdCurrent(con)){
     warning(paste("expired SQLiteConnection"))
     return(TRUE)
  }
  conId <- as(con, "integer")
  .Call("RS_SQLite_closeConnection", conId, PACKAGE = "RSQLite")
}

"sqliteConnectionInfo" <-
function(obj, what="", ...)
{
  if(!isIdCurrent(obj))
    stop(paste("expired", class(obj)))
  id <- as(obj, "integer")
  info <- .Call("RS_SQLite_connectionInfo", id, PACKAGE = "RSQLite")
  if(length(info$rsId)){
    rsId <- vector("list", length = length(info$rsId))
    for(i in seq(along = info$rsId))
      rsId[[i]] <- new("SQLiteResult", Id = c(id, info$rsId[i]))
    info$rsId <- rsId
  }
  if(!missing(what))
    info[what]
  else
    info
}

"sqliteExecStatement" <- 
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
#  if(dbGetInfo(out, what="isSelect")
#    out <- new("SQLiteResultSet", Id = rsId)
#  out
  out <- new("SQLiteResult", Id = rsId)
  out
}

## helper function: it exec's *and* retrieves a statement. It should
## be named somehting else.
"sqliteQuickSQL" <- 
function(con, statement)
{
   nr <- length(dbListResults(con))
   if(nr>0){                     ## are there resultSets pending on con?
      new.con <- dbConnect(con)   ## yep, create a clone connection
      on.exit(dbDisconnect(new.con))
      rs <- sqliteExecStatement(new.con, statement)
   } else rs <- sqliteExecStatement(con, statement)
   if(dbHasCompleted(rs)){
      dbClearResult(rs)            ## no records to fetch, we're done
      invisible()
      return(NULL)
   }
   res <- sqliteFetch(rs, n = -1)
   if(dbHasCompleted(rs))
      dbClearResult(rs)
   else 
      warning("pending rows")
   res
}

"sqliteFetch" <- 
function(res, n=0, ...)   
## Fetch at most n records from the opened resultSet (n = -1 meanSQLite
## all records, n=0 means extract as many as "default_fetch_rec",
## as defined by SQLiteDriver (see summary(drv, TRUE)).
## The returned object is a data.frame. 
## Note: The method dbHasCompleted() on the resultSet tells you whether
## or not there are pending records to be fetched. 
##
## TODO: Make sure we don't exhaust all the memory, or generate
## an object whose size exceeds option("object.size").  Also,
## are we sure we want to return a data.frame?
{    

  if(!isIdCurrent(res))
     stop("invalid result handle")
  type.convert <- function(x, na.strings="NA", as.is=TRUE, dec="."){
     .Internal(type.convert(x, na.strings, as.is, dec))
  }
  n <- as(n, "integer")
  rsId <- as(res, "integer")
  rel <- .Call("RS_SQLite_fetch", rsId, nrec = n, PACKAGE = "RSQLite")
  if(length(rel)==0 || length(rel[[1]])==0) 
    return(data.frame(NULL))
  for(j in seq(along = rel))
      rel[[j]] <- type.convert(rel[[j]], ...)
  ## create running row index as of previous fetch (if any)
  cnt <- dbGetRowCount(res)
  nrec <- length(rel[[1]])
  indx <- seq(from = cnt - nrec + 1, length = nrec)
  attr(rel, "row.names") <- as.character(indx)
  class(rel) <- "data.frame"
  rel
}

"sqliteResultInfo" <- 
function(obj, what = "", ...)
{
  if(!isIdCurrent(obj))
    stop(paste("expired", class(obj)))
   id <- as(obj, "integer")
   info <- .Call("RS_SQLite_resultSetInfo", id, PACKAGE = "RSQLite")
   flds <- info$fieldDescription[[1]]
   if(!is.null(flds)){
       flds$Sclass <- .Call("RS_DBI_SclassNames", flds$Sclass, 
                            PACKAGE = "RSQLite")
       flds$type <- .Call("RS_SQLite_typeNames", flds$type, 
                            PACKAGE = "RSQLite")
       ## no factors
       info$fields <- structure(flds, row.names = paste(seq(along=flds$type)), 
                                class="data.frame")
   }
   if(!missing(what))
     info[what]
   else
     info
}

"sqliteDescribeResult" <- 
function(obj, verbose = FALSE, ...)
{
  if(!isIdCurrent(obj)){
    show(obj)
    invisible(return(NULL))
  }
  show(obj)
  cat("  Statement:", dbGetStatement(obj), "\n")
  cat("  Has completed?", if(dbHasCompleted(obj)) "yes" else "no", "\n")
  cat("  Affected rows:", dbGetRowsAffected(obj), "\n")
  hasOutput <- as(dbGetInfo(obj, "isSelect")[[1]], "logical")
  flds <- dbColumnInfo(obj)
  if(hasOutput){
    cat("  Output fields:", nrow(flds), "\n")
    if(verbose && length(flds)>0){
       cat("  Fields:\n")  
       out <- print(flds)
    }
  }
  invisible(NULL)
}

"sqliteCloseResult" <- 
function(res, ...)
{
  if(!isIdCurrent(res)){
     warning(paste("expired SQLiteResult"))
     return(TRUE)
  }
  rsId <- as(res, "integer")
  .Call("RS_SQLite_closeResultSet", rsId, PACKAGE = "RSQLite")
}

"sqliteTableFields" <- 
function(con, name, ...)
{
   if(length(dbListResults(con))>0){
      con2 <- dbConnect(con)
      on.exit(dbDisconnect(con2))
   }
   else 
      con2 <- con
   rs <- dbSendQuery(con2, paste("select * from ", name))
   dummy <- fetch(rs, n = 1)
   dbClearResult(rs)
   nms <- names(dummy)
   if(is.null(nms))
      character()
   else
      nms
}
## this is exactly the same as ROracle's oraReadTable
"sqliteReadTable" <-  
function(con, name, row.names = "row_names", check.names = TRUE, ...)
## Should we also allow row.names to be a character vector (as in read.table)?
## is it "correct" to set the row.names of output data.frame?
## Use NULL, "", or 0 as row.names to prevent using any field as row.names.
{
   out <- try(dbGetQuery(con, paste("SELECT * from", name)))
   if(inherits(out, ErrorClass))
      stop(paste("could not find table", name))
   if(check.names)
       names(out) <- make.names(names(out), unique = TRUE)
   ## should we set the row.names of the output data.frame?
   nms <- names(out)
   j <- switch(mode(row.names),
           character = if(row.names=="") 0 else
               match(tolower(row.names), tolower(nms), 
                     nomatch = if(missing(row.names)) 0 else -1),
           numeric=, logical = row.names,
           NULL = 0,
           0)
   if(as.numeric(j)==0) 
      return(out)
   if(is.logical(j)) ## Must be TRUE
      j <- match(row.names, tolower(nms), nomatch=0) 
   if(j<1 || j>ncol(out)){
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

"sqliteWriteTable" <-
function(con, name, value, field.types, row.names = TRUE, 
  overwrite=FALSE, append=FALSE, ...)
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
    field.types <- sapply(value, dbDataType, dbObj = con)
  } 
  i <- match("row.names", names(field.types), nomatch=0)
  if(i>0) ## did we add a row.names value?  If so, it's a text field.
    field.types[i] <- dbDataType(con, field.types$row.names)
  names(field.types) <- make.db.names(con, names(field.types), allow.keywords=F)

  ## Do we need to clone the connection (ie., if it is in use)?
  if(length(dbListResults(con))!=0){ 
    new.con <- dbConnect(con)              ## there's pending work, so clone
    on.exit(dbDisconnect(new.con))
  } 
  else {
    new.con <- con
  }

  if(dbExistsTable(con,name)){
    if(overwrite){
      if(!dbRemoveTable(con, name)){
        warning(paste("table", name, "couldn't be overwritten"))
        return(FALSE)
      }
    }
    else if(!append){
      warning(paste("table", name, "exists in database: aborting dbWriteTable"))
      return(FALSE)
    }
  } 
  if(!dbExistsTable(con,name)){      ## need to re-test table for existance 
    ## need to create a new (empty) table
    sql1 <- paste("create table ", name, "\n(\n\t", sep="")
    sql2 <- paste(paste(names(field.types), field.types), collapse=",\n\t", 
                  sep="")
    sql3 <- "\n)\n"
    sql <- paste(sql1, sql2, sql3, sep="")
    rs <- try(dbSendQuery(new.con, sql))
    if(inherits(rs, ErrorClass)){
      warning("could not create table: aborting assignTable")
      return(FALSE)
    } 
    else 
      dbClearResult(rs)
  }

  fn <- tempfile("rsdbi")
  dots <- list(...)
  safe.write(value, file = fn, batch = dots$batch)
  on.exit(unlink(fn), add = TRUE)
  if(FALSE){
    sql4 <- paste("COPY '",name,"' FROM '",fn,"' USING DELIMITERS ','",sep="")
    rs <- try(dbSendQuery(new.con, sql4))
    if(inherits(rs, ErrorClass)){
      warning("could not load data into table")
      return(FALSE)
    } 
    else 
      dbClearResult(rs)
    TRUE
  }
  conId <- as(new.con, "integer")
  .Call("RS_SQLite_importFile", conId, name, fn, ",", PACKAGE = "RSQLite")
}
 
## from ROracle, except we don't quote strings here.
"safe.write" <- 
function(value, file, batch, ..., quote.string = FALSE)
## safe.write makes sure write.table don't exceed available memory by batching
## at most batch rows (but it is still slowww)
{  
   N <- nrow(value)
   if(N<1){
      warning("no rows in data.frame")
      return(NULL)
   }
   if(missing(batch) || is.null(batch))
      batch <- 10000
   else if(batch<=0) 
      batch <- N
   from <- 1 
   to <- min(batch, N)
   while(from<=N){
      if(usingR())
         write.table(value[from:to,, drop=FALSE], file = file, append = TRUE, 
               quote = quote.string, sep=",", na = .SQLite.NA.string, 
               row.names=FALSE, col.names=FALSE, eol = '\n', ...)
      else
         write.table(value[from:to,, drop=FALSE], file = file, append = TRUE, 
               quote.string = quote.string, sep=",", na = .SQLite.NA.string, 
               dimnames.write=FALSE, end.of.row = '\n', ...)
      from <- to+1
      to <- min(to+batch, N)
   }
   invisible(NULL)
}

##
## Utilities
##
"sqliteDataType" <- function(obj, ...)
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
