% $Id: dbConnect-methods.Rd,v 1.2 2003/11/04 16:41:02 dj Exp dj $
\name{dbConnect-methods}
\docType{methods}
\alias{dbDisconnect-methods}
\alias{dbConnect-methods}
\alias{dbDisconnect,SQLiteConnection-method}
\alias{dbConnect,SQLiteDriver-method}
\alias{dbConnect,SQLiteConnection-method}
\alias{dbConnect,character-method}
\title{
  Create a connection object to an SQLite DBMS
}
\description{
   These methods are straight-forward implementations of the corresponding
   generic functions.
}
\section{Methods}{\describe{
\item{drv}{
  an object of class \code{SQLiteDriver}, or
  the character string "SQLite" or an \code{SQLiteConnection}.
  }
\item{conn}{
  an \code{SQLiteConnection} object as produced by \code{dbConnect}.
  }
\item{\dots }{
  In the case of SQLite all you need is \code{dbname}, which
  should refer to a file name.
  }
}
}
\section{Side Effects}{
  A connection between R/S-Plus and the embeddable SQLite server is 
  established.  Note that since the SQLite is embedded in R/S-Plus,
  connections are not too resource hungry.

  SQLite connections only require the file name where the SQLite
  database reside.  For details see \code{\link{SQLite}}.
}
\references{
  See the Database Interface definition document
  \code{DBI.pdf} in the base directory of this package
  or \url{http://stat.bell-labs.com/RS-DBI}.
}
\seealso{
  \code{\link{SQLite}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbGetQuery}},
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
# create an SQLite instance and create one connection.
drv <- dbDriver("SQLite")

# open the connection using user, passsword, etc., as
con <- dbConnect(drv, dbname = "sqlite.db")

# Run an SQL statement by creating first a resultSet object
rs <- dbSendQuery(con, statement = paste(
                      "SELECT w.laser_id, w.wavelength, p.cut_off",
                      "FROM WL w, PURGE P", 
                      "WHERE w.laser_id = p.laser_id", 
                      "SORT BY w.laser_id")
# we now fetch records from the resultSet into a data.frame
data <- fetch(rs, n = -1)   # extract all rows
dim(data)
}
}
\keywords{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex
