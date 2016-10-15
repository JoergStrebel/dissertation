##############################################################################
#
# Bestimmt das optimale Deployment Target unter Berücksichtigung der 
# Kosten und des Outsourcing Degrees
#
##############################################################################

rm(list = ls()) 
library(MASS)
library(ROracle)

##############################################################################
#
# Initialisierung der Variablen und Faktorwerte
#
##############################################################################
set.seed(53)


##############################################################################
#
# Initialisierung
#
##############################################################################

# Hole Daten aus DB
drv <- dbDriver("Oracle")
con <- dbConnect(drv, username ="", password="",dbname = "XE")

experim.db<-dbGetQuery(con,"select EXPERIM, szenario.osdegr from SZENPROV,SZENARIO where SZENPROV.ACTIVEEXP=1 and SZENARIO.SID=SZENPROV.SID group by EXPERIM,szenario.osdegr order by EXPERIM")


for (k in 1:dim(experim.db)[1])
{
  EXPNR.val<-experim.db[k,1]
  OSDEGR.val<-experim.db[k,2]

  #Hole Optimierungsergebnisse  
  #sql.str<-paste("select SID,APP,EXPERIM,(COSTIAAS-COSTIH) COSTDIFF,TARGET,TARGETRAND, least(COSTIAAS-COSTIH,COSTIAASRES-COSTIH) MINCOSTDIFF from optappres where experim=",as.character(experim.db[k,1])," order by MINCOSTDIFF asc",sep="")
  OPTRES.df<-dbGetQuery(con,"select ROWID,SID,APP,EXPERIM,(COSTIAAS-COSTIH) COSTDIFF,TARGET,TARGETRAND, least(COSTIAAS-COSTIH,COSTIAASRES-COSTIH) MINCOSTDIFF from optappres where experim=:1 order by MINCOSTDIFF asc",data=data.frame(EXPNR.val))
  
  # Bestimme Anzahl der IaaS Apps
  nr.iaas<-as.integer(floor((dim(OPTRES.df)[1])*OSDEGR.val))
  #print(c("Anz. IaaS",as.character(nr.iaas))) 
  
  # Optimierung nach Kostendifferenz
  OPTRES.df[1:nr.iaas,"TARGET"]<-"iaas"
  OPTRES.df[(nr.iaas+1):(dim(OPTRES.df)[1]),"TARGET"]<-"inhouse"   
  
  # Zufällig Auswahl des Ziels
  rnd.idx<-sample(1:(dim(OPTRES.df)[1]),nr.iaas,replace=FALSE)
  OPTRES.df[1:(dim(OPTRES.df)[1]),"TARGETRAND"]<-"inhouse" # setze alles auf inhouse
  OPTRES.df[rnd.idx,"TARGETRAND"]<-"iaas" # setze die zufällig gezogenen auf iaas

  #Update Datenbank
  #for (l in 1:dim(OPTRES.df)[1])
  #{
    #APP.str<-paste("'",as.character(OPTRES.df[l,"APP"]),"'",sep="")
    #schreibe optimierte Auswahl
    #update.str<-paste("update OPTAPPRES set TARGET='",as.character(OPTRES.df[l,"TARGET"]),"' where APP=",APP.str," and SID=",OPTRES.df[l,"SID"]," and EXPERIM=",OPTRES.df[l,"EXPERIM"],sep="")     
    rs <- dbSendQuery(con, "update OPTAPPRES set TARGET= :1 where ROWID= :2",data = data.frame(TARGET=OPTRES.df[,"TARGET"],ROWID=OPTRES.df[,"ROWID"]))
    dbClearResult(rs)
    
    #schreibe zufällige  Auswahl
    #update.str<-paste("update OPTAPPRES set TARGETRAND='",as.character(OPTRES.df[l,"TARGETRAND"]),"' where APP=",APP.str," and SID=",OPTRES.df[l,"SID"]," and EXPERIM=",OPTRES.df[l,"EXPERIM"],sep="")     
    rs <- dbSendQuery(con, "update OPTAPPRES set TARGETRAND= :1 where ROWID= :2",data = data.frame(TARGET=OPTRES.df[,"TARGETRAND"],ROWID=OPTRES.df[,"ROWID"]))
    dbClearResult(rs)
    dbCommit(con)
  #}#apps
    
}# experim

# done with this connection
dbDisconnect(con)




