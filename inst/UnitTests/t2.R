library(RSQLite)
db = dbConnect(SQLite(), tempfile())

dbWriteTable(db, "dat2", "dat2.txt", header=T, sep="|") 


dbListTables(db)

dbGetQuery(db, "select * from dat2")


