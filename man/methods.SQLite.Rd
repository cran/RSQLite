\name{methods.SQLite}
\alias{SQLDataType.SQLiteConnection}
\alias{SQLDataType.SQLiteManager}
\alias{SQLDataType.default}
\alias{as.SQLiteConnection}
\alias{as.SQLiteManager}
\alias{as.SQLiteResultSet}
\alias{as.integer.SQLiteConnection}
\alias{as.integer.SQLiteManager}
\alias{as.integer.SQLiteResultSet}
\alias{as.integer.dbObjectId}
\alias{assignTable.SQLiteConnection}
\alias{close.SQLiteConnection}
\alias{close.SQLiteResultSet}
\alias{close.dbConnection}
\alias{close.resultSet}
\alias{commit.SQLiteConnection}
\alias{dbApply.SQLiteResultSet}
\alias{dbConnect.SQLiteConnection}
\alias{dbConnect.SQLiteManager}
\alias{dbConnect.default}
\alias{dbExec.SQLiteConnection}
\alias{dbExecStatement.SQLiteConnection}
\alias{describe.SQLiteConnection}
\alias{describe.SQLiteManager}
\alias{describe.SQLiteResultSet}
\alias{execStatement.SQLiteConnection}
\alias{existsTable.SQLiteConnection}
\alias{existsTable.dbConnection}
\alias{fetch.SQLiteResultSet}
\alias{format.SQLiteConnection}
\alias{format.SQLiteManager}
\alias{format.SQLiteResultSet}
\alias{format.dbObjectId}
\alias{getConnection.SQLiteConnection}
\alias{getConnection.SQLiteResultSet}
\alias{getConnection.default}
\alias{getConnections.SQLiteManager}
\alias{getCurrentDatabase.SQLiteConnection}
\alias{getDatabases.SQLiteConnection}
\alias{getException.SQLiteConnection}
\alias{getException.SQLiteResultSet}
\alias{getFields.SQLiteResultSet}
\alias{getInfo.SQLiteConnection}
\alias{getInfo.SQLiteManager}
\alias{getInfo.SQLiteResultSet}
\alias{getManager.SQLiteConnection}
\alias{getManager.SQLiteResultSet}
\alias{getNumCols.default}
\alias{getResultSets.SQLiteConnection}
\alias{getRowCount.SQLiteResultSet}
\alias{getRowsAffected}
\alias{getRowsAffected.SQLiteResultSet}
\alias{getStatement.SQLiteResultSet}
\alias{getTable.SQLiteConnection}
\alias{getTable.dbConnection}
\alias{getTableFields.SQLiteConnection}
\alias{getTableFields.SQLiteResultSet}
\alias{getTableIndices.SQLiteConnection}
\alias{getTables.SQLiteConnection}
\alias{getVersion.SQLiteConnection}
\alias{getVersion.SQLiteManager}
\alias{hasCompleted.SQLiteResultSet}
\alias{load.SQLiteManager}
\alias{load.default}
\alias{loadManager.SQLiteManager}
\alias{new.SQLiteManager}
\alias{new.SQLiteObject}
\alias{new.SQLiteResultSet}
\alias{new.dbObjectId}
\alias{new.default}
\alias{newConnection.SQLiteManager}
\alias{print.SQLiteConnection}
\alias{print.SQLiteManager}
\alias{print.SQLiteResultSet}
\alias{print.dbObjectId}
\alias{quickSQL.SQLiteConnection}
\alias{removeTable.SQLiteConnection}
\alias{removeTable.dbConnection}
\alias{rollback.SQLiteConnection}
\alias{show.default}
\alias{unload.SQLiteManager}

\title{Support SQLite methods}
\description{
  SQLite support methods. For details on what it does, see the 
  documentation of the generic function.
}
\usage{
<generic>.SQLiteManager(obj, ...)   
}
\arguments{
  \item{<generic>}{refers to an actual generic function name, e.g., 
        \code{dbConnect}}
  \item{obj}{is some kind of SQLite object}
  \item{\dots}{additional arguments}
}
\details{
  See \code{help(generic)} for a description of the
  functionality that this method provides in the
  context of the SQLite driver.
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
m <- dbManager("SQLite")
con <- dbConnect(m, user="opto", pass="pure-light", dbname="opto")
}
}
\keyword{interface}
\keyword{database}
% vim: syntax=tex
%  vim: syntax=tex
