\name{SQLDataType}
\alias{SQLDataType}
\title{
  Determine the SQL Data Type of an R/S object
}
\description{
Determine an (approximately) appropriate SQL data type for an R/S object
}
\usage{
SQLDataType(mgr, obj, ...)
}
\arguments{
\item{mgr}{
a \code{dbManager} object, e.g., \code{MySQLManager},
\code{OraManager}
}
\item{obj}{
R/S object whose SQL type we want to determine.
}
\item{\dots }{
any other parameters that individual methods may need.
}
}
\value{
A character string specifying the SQL data type for \code{obj}.
}
\details{
This is a generic function.  
}
\seealso{
\code{\link{isSQLKeyword}}
\code{\link{make.SQL.names}}
}
\examples{\dontrun{
ora <- dbManager("MySQL")
sql.type <- SQLDataType(ora, x)
}
}
\keyword{interface}
\keyword{database}
% docclass is function
% Converted by Sd2Rd version 1.15.2.1.
%  vim: syntax=tex
