\name{dbApply}
\alias{dbApply}
\title{Apply R/S functions to remote groups of DBMS rows (experimental)}
\description{
  Applies R/S functions to groups of remote DBMS rows without
  bringing an entire result set all at once.  The result set
  is expected to be sorted by the grouping field.
}
\usage{
dbApply(rs, ...)
}
\arguments{
  \item{rs}{a result set (see \code{\link{dbExec}}).}
  \item{...}{any additional arguments to be passed to \code{FUN}.}
}
\details{
  \code{dbApply} 
  This function is meant to handle somewhat gracefully(?) large amounts 
  of data from the DBMS by bringing into R manageable chunks (about 
  \code{batchSize} records at a time, but not more than \code{maxBatch}); 
  the idea is that the data from individual groups can be handled by R, but
  not all the groups at the same time.  
  
  The implementation allows us to register R functions that get invoked
  when certain fetching events occur. These include the ``begin'' event
  (no records have been yet fetched), ``begin.group'' (the record just 
  fetched belongs to a new group), ``new record'' (every fetched record
  generates this event), ``group.end'' (the record just fetched was the
  last row of the current group), ``end'' (the very last record from the
  result set). Awk and perl programmers will find this paradigm very
  familiar (although SAP's ABAP language is closer to what we're doing).
}
\value{
  A list with as many elements as there were groups in the
  result set.
}
\note{This is an experimental version.

The terminology that we're using is closer to SQL than R.  In R
what we're referring to ``groups'' are the individual levels of
a factor (grouping field in our terminology).
}

\seealso{\code{MySQL}, \code{\link{dbExec}}, \code{\link{fetch}}.}

\examples{\dontrun{
## compute quanitiles for each network agent
con <- dbConnect(MySQL(), group="vitalAnalysis")
rs <- dbExec(con, 
             "select Agent, ip_addr, DATA from pseudo_data order by Agent")
out <- dbApply(rs, INDEX = "Agent", 
        FUN = function(x, grp) quantile(x$DATA, names=F))
}
}
\keyword{programming}% at least one, from doc/KEYWORDS
\keyword{interface}% __ONLY ONE__ keyword per line
\keyword{database}
% vim: syntax=tex
