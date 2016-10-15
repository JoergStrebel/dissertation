##############################################################################
##
## INFO:
## -Unterschiedliche Server nutzen die gleichen DB-Daten
## - Ein Server hat nur genau eine SAP-SID
## - Erzeugt einen kompletten Versuchsplan, ohne Rücksicht auf die vorhandene DB-Füllung
## TODO:
##
## Vorgehen:
##   - lade Stammdaten in R-Strukturen (CTYPES, APPS)                  OK
##   - hole die maximale SID                                           OK
##   - setze Levels für die einzelnen unabh. Variablen                 OK
##   - erzeuge vollfaktoriellen Versuchsplan in einer R Tabelle        OK
##   - erstelle eine Routine zum Schreiben der SZENARIO Tabelle        OK
##   - schreibe den Versuchsplan in SZENARIO                           OK
##   - Erzeuge SZENPROVIDER                                            OK
##   - Erzeuge LOAD matrix                                             OK
##   - Schreibe LOAD matrix in DB                                      OK
##
##############################################################################

rm(list = ls()) 
library(MASS)
library(foreach)
library(doMC)
library(ROracle)

registerDoMC(4)


##############################################################################
##
## Funktion zur Berechnung der Semi-Varianz einer Ressourcendimension
## Data ist vector
## Normierung um den Mean - in anlehnung an den coefficient of variation (s/xbar)
#
##############################################################################
semivar <- function(vData, valp) 
{
  vDataC<-na.omit(vData)  
  Qa<-quantile(vDataC,probs=valp,na.rm = TRUE)
  Ma<-mean(vDataC,na.rm = TRUE)
  #print(Qa)  
  diffq<-vDataC-Qa
  diffq[diffq<0]<-0    
  return(sqrt(sum(diffq^2)/(length(vDataC)-1))/Ma)
} 

##############################################################################
##
## Funktion zur Berechnung der Funktion o()
## Data ist vector
##
##############################################################################
# oi <- function(gx.val,gxm.val, ALPHA.val) 
# {
#   # Oberer Elemente: resfreqs.vec[-1]
#   # Untere Elemente: resfreqs.vec[-(length(resfreqs.vec))]
#   
#   if (gx.val<=ALPHA.val & gxm.val<=ALPHA.val) # der Ast schneidet alles kleiner alpha ab
#   {
#     return(0)
#   }
#   else
#   {
#     if (gx.val>ALPHA.val & gxm.val<=ALPHA.val) 
#     {
#       return(0)
#     }
#     else
#     {
#       return(gx.val-gxm.val)
#     }
#   }
#   return(gx.val-gxm.val)
# } #oi 


##############################################################################
##
## Funktion zur Berechnung der empirischen Häufigkeit
## Parameter werden per value übergeben
## Parameter: CPU/RAM matrix, Instanzgröße CPU, Instanzgröße RAM
##
##############################################################################
Hemp <- function(CPURAM.mat, CPU.val, RAM.val) 
{
  #Spaltennamen: MEM_PHYS,CPU_COUNT

  GM.mat<-na.omit(CPURAM.mat) 
  GMcount.val<-dim(GM.mat)[1]
  #print(c('GMcount.val: ',GMcount.val))
  ZM.mat<-GM.mat[(GM.mat$MEM_PHYS/(1024*1024*1024))<=RAM.val & GM.mat$CPU_COUNT<=CPU.val,]    
  #print(c('ZMcount.val: ',dim(ZM.mat)[1]))
  ratio.val<-((dim(ZM.mat)[1])/GMcount.val)
  return(ratio.val)    
} 
# Testfälle
#MEM_PHYS<-c(20,30,NA,40,20)
#CPU_COUNT<-c(1,2,4,3,NA)
#test.df<-data.frame(MEM_PHYS,CPU_COUNT)
#Hemp(test.df,3,20)
#Hemp(test.df,4,30)

##############################################################################
##
## Initialisierung der Variablen und Faktorwerte
## Anzahl Szenarien=11*11*3*8*7 = 20328
##
##############################################################################
set.seed(53)

# großer Ansatz
QALPHA <-c(0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1)     #11
PDELTA<-c(0.5,0.6,0.7,0.8,0.9,1,1.1,1.2,1.3,1.4,1.5)  #11
LNORM<-c(1,2,-1)     #3 Inf is coded as -1
OSDEGR<-c(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8)   #8
QOMEGA <-max(QALPHA)               #1
ADELTA<-c(0.7,0.8,0.9,1,1.1,1.2,1.3)  #7

#kleiner Ansatz
# QALPHA <-c(0.7,0.8,1)     #8
# PDELTA<-c(0.9,1,1.1)  #8
# LNORM<-c(1,2)     #4 Inf is coded as -1
# OSDEGR<-c(0.1,0.2,0.8)   #8
# QOMEGA <-max(QALPHA)               #1
# ADELTA<-c(0.8,0.9,1,1.1)  #8

##############################################################################
##
## Hole Daten aus DB
##
##############################################################################

## create an Oracle instance and create one connection.
drv <- dbDriver("Oracle")
con <- dbConnect(drv, username ="", password="",dbname = "XE")

#sid.max.db<-as.numeric(sqlQuery(channel, "select max(sid) from szenario"))
sid.max.db<-0 #Wegwerf-DB-Besatz
ress.ctypes.db<-dbGetQuery(con, "select CTYPE from CTYPES where CTYPE<>'Instance' order by CTYPE asc")
apps.db<-dbGetQuery(con,"select * from APPS")
clients.db<-dbGetQuery(con, "select pid from provider where type='client' order by pid asc")
providers.db<-dbGetQuery(con, "select pid from provider where type='provider' order by pid asc")
load.db<-dbGetQuery(con,"select * from V_PDATA")
storhost.db<-dbGetQuery(con,"select * from V_SHOSTDATA")
ldata<-load.db[,c("EVENT_ID","SYSTEM", "CPU_COUNT","LAN_IN","LAN_OUT","MEM_PHYS")]
shdata<-storhost.db[,c("SAPSID","FILLED")]
instance.db<-dbGetQuery(con,"select t2.pid,t1.SVID,t1.CORES,t1.RAM from instance t1, provider t2 where t1.pid=t2.pid order by t2.pid,t1.SVID ASC")

print("Loaded Data")
##############################################################################
##
## Baue SZENARIO-Tabelle
##
##############################################################################
vp.comb<-expand.grid(QALPHA=QALPHA,PDELTA=PDELTA,LNORM=LNORM,OSDEGR=OSDEGR,QOMEGA=QOMEGA,ADELTA=ADELTA, stringsAsFactors=FALSE)
SID<-seq(from = sid.max.db+1, to = sid.max.db+dim(vp.comb)[1], by = 1)
vp<-data.frame(SID,vp.comb,stringsAsFactors=FALSE) # CREATED is set when the rows are written to the DB

##############################################################################
##
## Schreibe SZENARIO in die DB
##
##############################################################################
#SID,DESCRI,QALPHA,CREATED,LNORM,OSDEGR,QOMEGA,PDELTA

DESCRI.str<-"Test 3"
curdate<-Sys.Date()

all.szen.data<-data.frame(vp,rep(DESCRI.str,dim(vp)[1]),rep(curdate,dim(vp)[1]))

rs <- dbSendQuery(con, "insert into SZENARIO(SID,QALPHA,PDELTA,LNORM,OSDEGR,QOMEGA,ADELTA,DESCRI,CREATED) values (:1, :2, :3, :4, :5, :6, :7, :8, :9)",data=all.szen.data)
dbClearResult(rs)

# ok, everything looks fine
dbCommit(con)

print("Wrote Scenario")
##############################################################################
##
## Baue SZENPROVIDER-Tabelle
## Es wird jeweils der erste Client, Provider genommen
##
##############################################################################

szenprov.comb<-expand.grid(SID=SID,PIAAS=providers.db[,1],PINHOUSE=clients.db[,1],stringsAsFactors=FALSE)
EXPERIM.vec<-seq(from = 1, to = dim(szenprov.comb)[1], by = 1)
EXPERIM.mat<-data.frame(EXPERIM.vec,szenprov.comb) # CREATED is set when the rows are written to the DB

for (k in 1:length(EXPERIM.vec))
{
  #   sqlvalues.str<-paste(EXPERIM.mat[k,1],",",EXPERIM.mat[k,"SID"],",'",EXPERIM.mat[k,"PINHOUSE"],"'",",'",EXPERIM.mat[k,"PIAAS"],"',",1,sep="")
  #   sql.str<-paste(insert.prov.str,sqlvalues.str,end.str,sep="")

  rs <- dbSendQuery(con,"insert into SZENPROV(EXPERIM,SID,PINHOUSE,PIAAS,ACTIVEEXP) values (:1,:2,:3,:4,:5)",data=data.frame(EXPERIM.mat[k,1],EXPERIM.mat[k,"SID"],EXPERIM.mat[k,"PINHOUSE"],EXPERIM.mat[k,"PIAAS"],c(1)))
  dbClearResult(rs)
}
dbCommit(con)

print("Wrote Experiments")
##############################################################################
##
## LOAD Matrix Erstellung
##
##############################################################################
# Format: SID,CTYPE,APP,EXPVAL,QOVAL
# Es müssen nicht mehr alle Werte berechnet werden, da die Instanzauswahl über die Frequenz läuft
load.comb<-expand.grid(QALPHA=QALPHA,QOMEGA=QOMEGA,APP=apps.db[,"APP"],CTYPE=ress.ctypes.db[,1],stringsAsFactors=FALSE)

quantresult.mat<-foreach (k=1:dim(load.comb)[1], .combine=rbind) %dopar%
{
    # ALPHA, OMEGA, SAPSID herausfinden
    ALPHA.val<-load.comb[k,"QALPHA"]
    OMEGA.val<-load.comb[k,"QOMEGA"]
    SAPSID.val<-ldata[ldata$EVENT_ID==load.comb[k,"APP"],"SYSTEM"][1]
    
    EXPVAL.val<-switch(as.character(load.comb[k,"CTYPE"]),
    RAM = mean(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"MEM_PHYS"],na.rm=TRUE),  
    CPU = mean(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"CPU_COUNT"],na.rm=TRUE),
    Storage = mean(shdata[shdata$SAPSID==SAPSID.val,"FILLED"],na.rm=TRUE),
    Network_in = mean(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"LAN_IN"],na.rm=TRUE),
    Network_out = mean(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"LAN_OUT"],na.rm=TRUE),
    stop("Resource not found"))
    
    QAVAL.val<-switch(as.character(load.comb[k,"CTYPE"]),
    RAM = quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"MEM_PHYS"],probs=c(ALPHA.val),na.rm=TRUE), 
    CPU = quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"CPU_COUNT"],probs=c(ALPHA.val),na.rm=TRUE),
    Storage =  quantile(shdata[shdata$SAPSID==SAPSID.val,"FILLED"],probs=c(ALPHA.val),na.rm=TRUE),
    Network_in =  quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"LAN_IN"],probs=c(ALPHA.val),na.rm=TRUE),
    Network_out =  quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"LAN_OUT"],probs=c(ALPHA.val),na.rm=TRUE),
    stop("Resource not found"))
        
    QOVAL.val<-switch(as.character(load.comb[k,"CTYPE"]),
    RAM = quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"MEM_PHYS"],probs=c(OMEGA.val),na.rm=TRUE), 
    CPU = quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"CPU_COUNT"],probs=c(OMEGA.val),na.rm=TRUE),
    Storage = quantile(shdata[shdata$SAPSID==SAPSID.val,"FILLED"],probs=c(OMEGA.val),na.rm=TRUE),
    Network_in = quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"LAN_IN"],probs=c(OMEGA.val),na.rm=TRUE),
    Network_out = quantile(ldata[ldata$EVENT_ID==load.comb[k,"APP"],"LAN_OUT"],probs=c(OMEGA.val),na.rm=TRUE),
    stop("Resource not found"))

    return(c(EXPVAL.val,QAVAL.val, QOVAL.val))
}

load.comb<-data.frame(load.comb,quantresult.mat)
print(dim(load.comb))
print(dim(quantresult.mat))
print("Calculated Load")
##############################################################################
##
## Schreibe LOAD-Tabelle
##
##############################################################################
# Name und Reihenfolge:  SID,CTYPE,APP,EXPVAL,QOVAL,SEMIVAR

# 1. Stelle sicher, dass alle Spalten vorhanden sind
SEMIVAR<-rep(NA,dim(load.comb)[1])
load.comb<-data.frame(load.comb,SEMIVAR)
# 2. Benenne DF-Spalten nach den DB-Spalten
colnames(load.comb)<-c("QALPHA","QOMEGA","APP","CTYPE","EXPVAL","QAVAL","QOVAL","SEMIVAR")

# execute and bind an INSERT statement
rs <- dbSendQuery(con, "insert into LOAD(QALPHA,QOMEGA,APP,CTYPE,EXPVAL,QAVAL,QOVAL,SEMIVAR) values (:1, :2, :3, :4, :5, :6, :7, :8)",data = load.comb)
dbClearResult(rs)
# ok, everything looks fine
dbCommit(con)



##############################################################################
##
## Instanz-Frequenz-Berechnung
## Es werden die Anteile für alle Instanzen berechnet
## ACHTUNG: BMW virt. Server 6x6 gelöscht wegen inkonsistenzen - TODO: in der Kostenberechnung nachziehen!
##############################################################################

# Erzeuge alle Experiment-App-Kombinationen
instfreq.comb<-expand.grid(PROVIDER=c(providers.db[,1],clients.db[,1]),ALPHA=QALPHA,APP=apps.db[,"APP"],stringsAsFactors=FALSE)

for (combrun in 1:dim(instfreq.comb)[1])
{
  #Hole den gültigen Provider
  tmpprov.val<-instfreq.comb[combrun,"PROVIDER"]
  
  # Hole die für den aktuellen Provider gültigen Daten
  provinst.mat<-instance.db[instance.db$PID==tmpprov.val,]
  
  #Hole ALPHA
  tmpALPHA.val<-instfreq.comb[combrun,"ALPHA"]
  
  # Lastdaten
  tmLoad.mat<-ldata[ldata$EVENT_ID==instfreq.comb[combrun,"APP"],c("MEM_PHYS","CPU_COUNT")] 
  
  # Berechne Häufigkeiten
  resfreqs.vec<-apply(provinst.mat[,c("CORES","RAM")],1,function(x) Hemp(tmLoad.mat,x[1],x[2]))

  # Berechne oi()
  # Trick: Um die Differenz der aufeinanderfolgenden Were zu erhalten, wird das jeweils erste und letzte Element des Frequenzvektors entfernt
  #difffreqs.vec<-apply(data.frame(resfreqs.vec[-1],resfreqs.vec[-(length(resfreqs.vec))]),1,function(x) oi(x[1],x[2],tmpALPHA.val))       
  #difffreqs.vec<-c(0,difffreqs.vec) #FEHLER!!!
  difffreqs.vec<-c(resfreqs.vec[1],resfreqs.vec[-1]-resfreqs.vec[-length(resfreqs.vec)])

  # Schreibe DB  
  for (k in 1:dim(provinst.mat)[1])
  {

    freq.df<-data.frame(c(instfreq.comb[combrun,"APP"]),c(provinst.mat[k,"SVID"]),c(tmpprov.val),c(tmpALPHA.val),c(difffreqs.vec[k]),c(resfreqs.vec[k]))
    colnames(freq.df)<-c("APP","SVID","PROVIDER","QALPHA","FREQ","CUMFREQ")
    
    # execute and bind an INSERT statement
    # Spalten: COEFVAR,CTYPE,APP,ALPHA.val
    rs <- dbSendQuery(con, "insert into INSTFREQ(APP,SVID,PROVIDER,ALPHA, FREQ, CUMFREQ) values (:1,:2,:3,:4,:5, :6)",data = freq.df)
    dbClearResult(rs)
  } #k    
}#combrun

dbCommit(con)

print("Wrote Frequencies")
##############################################################################
##
## Semivarianz-Berechnung
##
##############################################################################

szenario.db<-dbGetQuery(con,"select CTYPE,APP, qalpha from LOAD group by CTYPE,APP,qalpha order by qalpha,app")


for (k in 1:dim(szenario.db)[1])
{
    # ALPHA, OMEGA, SAPSID herausfinden
    ALPHA.val<-szenario.db[k,"QALPHA"]
    SAPSID.val<-ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"SYSTEM"][1]
    
    SEMIVAR.val<-switch(as.character(szenario.db[k,"CTYPE"]),
    RAM = semivar(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"MEM_PHYS"],c(ALPHA.val)), 
    CPU = semivar(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"CPU_COUNT"],c(ALPHA.val)),
    Storage = semivar(shdata[shdata$SAPSID==SAPSID.val,"FILLED"],c(ALPHA.val)),
    Network_in = semivar(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"LAN_IN"],c(ALPHA.val)),
    Network_out = semivar(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"LAN_OUT"],c(ALPHA.val)),
    stop("Resource not found"))
    
    semivar.df<-data.frame(SEMIVAR=SEMIVAR.val,CTYPE=szenario.db[k,"CTYPE"],APP=szenario.db[k,"APP"],QALPHA=ALPHA.val)
    #print(semivar.df)
    # execute and bind an INSERT statement
    # Spalten: SEMIBVAR.val,CTYPE,APP,ALPHA.val
    rs <- dbSendQuery(con, "update LOAD set semivar= :1 where CTYPE= :2 and APP= :3 and ABS(QALPHA -:4)<0.001",data = semivar.df)
    #print(rs)
    dbClearResult(rs)
}
dbCommit(con)

##############################################################################
##
## Befüllung des Coefficient of Variation
##
##############################################################################

for (k in 1:dim(szenario.db)[1])
{
    # ALPHA, OMEGA, SAPSID herausfinden
    SAPSID.val<-ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"SYSTEM"][1]
    
    COEFVAR.val<-switch(as.character(szenario.db[k,"CTYPE"]),
    RAM = sd(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"MEM_PHYS"],na.rm=TRUE)/mean(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"MEM_PHYS"],na.rm=TRUE), 
    CPU = sd(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"CPU_COUNT"],na.rm=TRUE)/mean(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"CPU_COUNT"],na.rm=TRUE),
    Storage = sd(shdata[shdata$SAPSID==SAPSID.val,"FILLED"],na.rm=TRUE)/mean(shdata[shdata$SAPSID==SAPSID.val,"FILLED"],na.rm=TRUE),
    Network_in = sd(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"LAN_IN"],na.rm=TRUE)/mean(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"LAN_IN"],na.rm=TRUE),
    Network_out = sd(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"LAN_OUT"],na.rm=TRUE)/mean(ldata[ldata$EVENT_ID==szenario.db[k,"APP"],"LAN_OUT"],na.rm=TRUE),
    stop("Resource not found"))

    coef.df<-data.frame(c(COEFVAR.val),c(szenario.db[k,"CTYPE"]),c(szenario.db[k,"APP"]))
    colnames(coef.df)<-c("COEFVAR","CTYPE","APP")
    
    # execute and bind an INSERT statement
    # Spalten: COEFVAR,CTYPE,APP,ALPHA.val
    rs <- dbSendQuery(con, "update LOAD set COEFVAR= :1 where CTYPE= :2 and APP= :3",data = coef.df)
    dbClearResult(rs)
}
dbCommit(con)

# done with this connection
dbDisconnect(con)
print("Wrote Semivariance") 

 




