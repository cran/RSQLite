\name{SQLite}
\alias{SQLite}
\alias{SQLiteManager}
\title{
  Instantiate the SQLite engine from the current R/S session.
}
\description{
This function creates and initializes the SQLite engine.
It returns an object that allows you to connect to the SQLite 
embedded engine.
}
\usage{
SQLite(max.con = 10, fetch.default.rec = 500, force.reload = F)
}
\arguments{
\item{max.con }{
  maximum number of connections that we intended to have open.
  This can be up to 100, a limit defined at compilation time.
  }
\item{fetch.default.rec}{
  number of records to fetch at one time from the database.
  (The \code{fetch} method uses this number as a default.)
  }
\item{force.reload}{
  should we reload (reinitialize) the package code?
  Setting this to \code{TRUE} allows you to change
  default settings.  Notice that all connections should be closed
  before re-loading.
  }
}
\value{
An object \code{SQLiteManager} that extends 
\code{dbManager} and 
\code{dbObjectId}.
This object is required to create connections
to the embedded SQLite database.
There can be many SQLite database instances running
simultaneously.
}
\section{Side Effects}{
The R/S client part of the database communication is initialized,
but note that connecting to database instances needs to be done through
calls to \code{dbConnect}.
}
\details{
This object is a singleton, that is, on subsequent invocations
it returns the same initialized object. 

This implementation allows the R/S embedded SQLite engine
to work with multiple database instances through multiple connections 
simultaneously.

SQLite keeps each database instance in one single file. The name
of the database \emph{is} the file name, thus database names
should be legal file names in the running platform.
}
\section{User authentication}{
SQLite is a single-user database engine, so it has no concept of
user.
}
\section{References}{
See the Omega Project for Statistical Computing
at \url{http://www.omegahat.org}
for more details on the R/S database interface.

See the Adobe PDF file \code{RS-DBI.pdf} under the \code{doc}
subdirectory of this package, i.e., 
\code{.path.package("RSQLite")/doc/RS-DBI.pdf}.

See the documentation at the SQLite Web site
\url{http://www.hwaci/com/sw/sqlite/index.html} for details.
}
\seealso{
On database managers:

\code{\link{dbManager}}
\code{\link{SQLite}}
\code{\link{load}}
\code{\link{unload}}

On connections, SQL statements and resultSets:

\code{\link{dbExecStatement}}
\code{\link{dbExec}}
\code{\link{fetch}}
\code{\link{quickSQL}}

On transaction management:

\code{\link{commit}}
\code{\link{rollback}}

On meta-data:

\code{\link{describe}}
\code{\link{getVersion}}
\code{\link{getDatabases}}
\code{\link{getTables}}
\code{\link{getFields}}
\code{\link{getCurrentDatabase}}
\code{\link{getTableIndices}}
\code{\link{getException}}
\code{\link{getStatement}}
\code{\link{hasCompleted}}
\code{\link{getRowCount}}
\code{\link{getAffectedRows}}
\code{\link{getNullOk}}
\code{\link{getInfo}}
}
\examples{\dontrun{
   # create a SQLite instance and create one connection.
   m <- dbManager("SQLite")
   
   # initialize a new database "base.dbms" in the current directory
   # and copy some data.frame from the base package into it
   
   con <- dbConnect(m, dbname = "base.dbms")
   data(USArrests)
   assignTable(con, "USArrests", USArrests, overwrite = T)
   
   # query
   rs <- dbExecStatement(con, "select * from USArrests")
   d1 <- fetch(rs, n = 10)      # extract data in chunks of 10 rows
   hasCompleted(rs)
   d2 <- fetch(rs, n = -1)      # extract all remaining data
   hasCompleted(rs)
   close(rs)
   getTables(con)    
   }
}
\keyword{interface}
\keyword{database}
% docclass is function
%  vim: syntax=tex
