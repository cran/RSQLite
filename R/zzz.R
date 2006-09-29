## $Id: zzz.R 188 2006-09-25 19:07:47Z sethf $
".First.lib" <- 
function(libname, pkgname)
{
   ## need to dyn.load sqlite.dll before we attempt to load
   ## RSQLite.dll  -- there's got to be a better way...

   if(.Platform$OS.type=="windows"){
      if(!is.loaded("sqlite3_libversion"))
         dyn.load(system.file("libs", "sqlite3.dll", package = "RSQLite"))
   }
   library.dynam("RSQLite", package=pkgname)
}
