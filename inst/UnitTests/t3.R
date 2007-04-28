library(RSQLite)
db = dbConnect(SQLite(), tempfile())

dbWriteTable(db, "dat", "dat3.txt", header=T, sep="|", 
	     eol="\r\n")

dbListTables(db)

dbGetQuery(db, "select * from dat")


