library(RSQLite)
db = dbConnect(SQLite(), tempfile())

dbWriteTable(db, "dat1", "dat1.txt", header=T, sep="|") 


dbListTables(db)

dbGetQuery(db, "select * from dat1")


