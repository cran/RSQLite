% $Id: dbCallProc-methods.Rd,v 1.2 2003/11/04 16:41:02 dj Exp dj $
\name{dbCallProc-methods}
\docType{methods}
\alias{dbCallProc-methods}
\alias{dbCallProc,SQLiteConnection-method}
\title{
  Call an SQL stored procedure
}
\description{
  Not yet implemented.
}
\section{Methods}{\describe{
\item{conn}{
  a \code{SQLiteConnection} object.
  }
\item{\dots }{
  additional arguments are passed to the implementing method.
  }
}
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
\keywords{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex
