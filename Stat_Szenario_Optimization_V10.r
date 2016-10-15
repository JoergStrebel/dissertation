## t2.COSTOINSTIAAS sind die Kosten der maximal großen Instanz. Stellvertretergröße für kombinierten CPU/RAM-Bedarf
## t2.COSTOINSTIAAS/t2.COSTIAAS OINSTDYNRATIO

## Fragen: 
## 1. Wie häufig wird COSTOINSTIAAS überhaupt relevant, da es nur in COSTIAASRES landet (Entscheidungskriterium least(COSTIAAS-COSTIH,COSTIAASRES-COSTIH)): 3% der Fälle!


## t2.COSTIAAS/t2.COSTIH DYNRATIO,
## t2.COSTIAASRES/t2.COSTIH RESRATIO,
## t2.COSTOINSTIAAS/t2.COSTINSTIH OINSTRATIO,
## t2.COSTAINSTIAAS/t2.COSTINSTIH AINSTRATIO,
## t2.COSTDYNINSTIAAS/t2.COSTINSTIH DYNINSTRATIO,
## (t2.COSTIAAS+t2.COSTIAASRES+t2.COSTIH)/3 CASEWEIGHT,
## t2.COSTSTOIAAS/t2.COSTSTOIH STORATIO,
## t2.COSTAINSTIAAS/t2.COSTIAAS AINSTDYNRATIO,
## t2.COSTSTOIAAS/t2.COSTIAAS STODYNRATIO,
## t2.COSTNWIIAAS/t2.COSTIAAS NWIDYNRATIO,
## t2.COSTNWOIAAS/t2.COSTIAAS NWODYNRATIO,
## t2.COSTAINSTIAAS/t2.COSTIAASRES AINSTRESRATIO,
## t2.COSTOINSTIAAS/t2.COSTIAASRES OINSTRESRATIO,
## t2.COSTSTOIAAS/t2.COSTIAASRES STORESRATIO,
## t2.COSTNWIIAAS/t2.COSTIAASRES NWIRESRATIO,
## t2.COSTNWOIAAS/t2.COSTIAASRES NWORESRATIO,
## t2.COSTINSTIH/t2.COSTIH INSTIHRATIO,
## t2.COSTSTOIH/t2.COSTIH STOIHRATIO


##############################################################################
## Berechnet Tabelle, die pro APP, Experiment und SID die Kosten pro Kostenart auflistet
## TODO:
## - Check whether tariff logic is completely implemented; the min(ondem, res) should be respected everywhere
## 
##############################################################################
rm(list = ls()) 
library(MASS)
library(ROracle)

##############################################################################
##
## Initialisierung der Variablen und Faktorwerte
##
##############################################################################
set.seed(53)


##############################################################################
##
## Funktion zu Berechnung der Norm des Variabilitätsvektors
## vData is a vector whose elements are used for norm calculation
## iOrder is -1 for maximum norm
##
##############################################################################
lnorm <- function(X, mData, iOrder) 
{
  if (iOrder>0) 
  {
    return((sum(mData[mData$APP==X,-1]^iOrder))^(1/iOrder))
  }
  else
  {
    return(max(abs(mData[mData$APP==X,-1])))    
  }
} 


##############################################################################
#
# Initialisierung
#
##############################################################################


# Laufe durch die SZENARIO Tabelle aus der DB
#SZENARIO(SID,DESCRI,QALPHA,ACTIVESZEN,CREATED,LNORM,OSDEGR,QOMEGA,PDELTA) 
# create an Oracle instance and create one connection.
drv <- dbDriver("Oracle")
con <- dbConnect(drv, username ="", password="",dbname = "XE")
experim.db<-dbGetQuery(con, "select EXPERIM,PIAAS,PINHOUSE,QALPHA,QOMEGA,LNORM,OSDEGR,PDELTA,SZENARIO.SID,ADELTA from SZENPROV,SZENARIO where SZENPROV.ACTIVEEXP=1 and SZENARIO.SID=SZENPROV.SID order by EXPERIM")
instance.df<-dbGetQuery(con,"select svid,pid,value,valueares,valuebres, breakeven from tariff where tariff.ctype='Instance'")
is.factor(instance.df[,"PID"])

for (k in 1:dim(experim.db)[1])
{
  
  #Hole Szenariowerte und belege Variablen
  SID.val<-experim.db[k,"SID"]
  ALPHA.val<-experim.db[k,"QALPHA"]
  OMEGA.val<-experim.db[k,"QOMEGA"]
  LNORM.val<-experim.db[k,"LNORM"]
  OSDEGR.val<-experim.db[k,"OSDEGR"]
  PDELTA.val<-experim.db[k,"PDELTA"] 
  ADELTA.val<-experim.db[k,"ADELTA"]   
  CLIENT.val<-experim.db[k,"PINHOUSE"]
  PROVIDER.val<-experim.db[k,"PIAAS"]
  
  #Hole Load
  #LOAD.df<-dbGetQuery(con,"select CTYPE,APP,EXPVAL,QAVAL,QOVAL,SEMIVAR from load where QALPHA= :1 and QOMEGA= :2",data=data.frame(QALPHA=ALPHA.val,QOMEGA=OMEGA.val))

  ################################################################
  ## Berechnung der Basis-Instanzkosten!!
  ## Kein Minimumsfunktion des Tarifes notwendig, da für die gleiche Instanz für die gesamte Laufzeit 
  ## die Ondemand-Kosten immer größer als die Reserved-Kosten sein werden. 
  ################################################################
  #  Berücksichtigung der Reserved Kosten

  # -- Basiskosten für ALPHA
  # -- Instanzen werden zu 100% belegt
  # -- welche einheit wird in der reserved instance formel benötigt? 
  # --> kann nur Prozent sein wg. FREQ!!
  # select 
  #   t1.app,
  #   t2.instnr,
  #   t1.freq,
  #   t3.breakeven,
  #   case when t1.freq>t3.breakeven then t3.valuebres+t3.valueares else t3.value end INSTCOST
  # from 
  # instfreq t1, 
  # (select app,min(svid) INSTNR from instfreq where cumfreq>= 0.1 and provider= 'bmw' and alpha= 0.1 group by app) t2,
  # (select svid,pid,value,valueares,valuebres, breakeven from tariff where tariff.ctype='Instance') t3
  # where 
  # t1.app=t2.app and 
  # t2.instnr=t1.svid and 
  # t1.provider= 'bmw' and 
  # t1.alpha= 0.1 and
  # t3.svid=t1.svid and
  # t1.provider=t3.pid
  # order by t1.app; 

  ## Basis-Berechnung für BMW, OMEGA
  CLIENTINST.cost.df<-dbGetQuery(con,"select t1.app,t2.instnr,case when t1.freq>t3.breakeven then t3.valuebres+t3.valueares else t3.value end INSTCOST from instfreq t1,(select app,min(svid) INSTNR from instfreq where cumfreq>= :1 and provider= :2 and ABS(alpha- :3)<0.001 group by app) t2,(select svid,pid,value,valueares,valuebres, breakeven from tariff where tariff.ctype='Instance') t3 where t1.app=t2.app and t2.instnr=t1.svid and t1.provider= :4 and abs(t1.alpha- :5)<0.001 and t3.svid=t1.svid and t1.provider=t3.pid order by t1.app asc",data=data.frame(CUMFREQ=OMEGA.val, PROVIDER=CLIENT.val, ALPHA=OMEGA.val,CLIENT.val,OMEGA.val))
  Names.CLIENTINST.cost.df<-c("APP","NRINSTIH","COSTINSTIH")
  colnames(CLIENTINST.cost.df)<-Names.CLIENTINST.cost.df
  
  ## Basis-Berechnung für AMA, ALPHA
  ALPHAINST.cost.df<-dbGetQuery(con,"select t1.app,t2.instnr,case when t1.freq>t3.breakeven then t3.valuebres+t3.valueares else t3.value end INSTCOST from instfreq t1,(select app,min(svid) INSTNR from instfreq where cumfreq>= :1 and provider= :2 and ABS(alpha- :3)<0.001 group by app) t2,(select svid,pid,value,valueares,valuebres, breakeven from tariff where tariff.ctype='Instance') t3 where t1.app=t2.app and t2.instnr=t1.svid and t1.provider= :4 and ABS(t1.alpha- :5)<0.001 and t3.svid=t1.svid and t1.provider=t3.pid order by t1.app asc",data=data.frame(CUMFREQ=ALPHA.val, PROVIDER=PROVIDER.val, ALPHA=ALPHA.val,PROVIDER.val,ALPHA.val))
  Names.ALPHAINST.cost.df<-c("APP","NRAINSTIAAS","COSTAINSTIAAS")
  colnames(ALPHAINST.cost.df)<-Names.ALPHAINST.cost.df

  ## Basis-Berechnung für AMA, OMEGA
  OMEGAINST.cost.df<-dbGetQuery(con,"select t1.app,t2.instnr,case when t1.freq>t3.breakeven then t3.valuebres+t3.valueares else t3.value end INSTCOST from instfreq t1,(select app,min(svid) INSTNR from instfreq where cumfreq>= :1 and provider= :2 and ABS(alpha- :3)<0.001 group by app) t2,(select svid,pid,value,valueares,valuebres, breakeven from tariff where tariff.ctype='Instance') t3 where t1.app=t2.app and t2.instnr=t1.svid and t1.provider= :4 and ABS(t1.alpha- :5)<0.001 and t3.svid=t1.svid and t1.provider=t3.pid order by t1.app asc",data=data.frame(CUMFREQ=OMEGA.val, PROVIDER=PROVIDER.val, ALPHA=OMEGA.val,PROVIDER.val,OMEGA.val))
  Names.OMEGAINST.cost.df<-c("APP","NROINSTIAAS","COSTOINSTIAAS")
  colnames(OMEGAINST.cost.df)<-Names.OMEGAINST.cost.df
  
  ################################################################
  ## Berücksichtigung der On-Demand Instanz-Kosten für den elastischen Tariff
  ##
  ################################################################
  #   sql.str<-paste("select t1.app,sum(t4.value*t1.freq) ICOST from ","instfreq t1,szenario t2, tariffmap t3,tariff t4 ",
  # "where t2.sid=",as.character(SID.val) ," and t1.alpha=t2.qalpha and t3.svid=t1.svid and t3.tid=t4.tid and t1.provider='",as.character(PROVIDER.val),"' and t4.pid=t1.provider and t3.cvar='ondemand' and t4.ctype='Instance' ",
  # "group by t1.app order by t1.app asc",sep="")
  #   ONDEM.cost.vec<-sqlQuery(channel,sql.str,stringsAsFactors=FALSE)

  ONDEM.cost.df<-dbGetQuery(con,"select t4.app, nvl(t5.DYNCOST,0) DYNCOST from apps t4 full outer join (select t1.app,sum(case when t1.freq>t3.breakeven then t1.freq*(t3.valuebres+t3.valueares) else t1.freq*t3.value end) DYNCOST from instfreq t1,(select app,min(svid) INSTNR from instfreq where cumfreq>= :1 and provider= :2 and ABS(alpha- :3)<0.001 group by app) t2, (select svid,pid,value,valueares,valuebres, breakeven from tariff where tariff.ctype='Instance') t3 where t1.app=t2.app and t2.instnr<t1.svid and t1.provider= :4 and ABS(t1.alpha- :5)<0.001 and t3.svid=t1.svid and t1.provider=t3.pid group by t1.app) t5 on t4.app=t5.app order by t4.app asc",data=data.frame(CUMFREQ=ALPHA.val, PROVIDER=PROVIDER.val, ALPHA=ALPHA.val,PROVIDER.val,ALPHA.val))
  Names.ONDEM.cost.df<-c("APP","COSTDYNINSTIAAS")
  colnames(ONDEM.cost.df)<-Names.ONDEM.cost.df

  ################################################################
  ## Berücksichtigung der variablen Kosten für Storage etc.
  ##
  ################################################################
  # Storage, NW Kosten
  # IaaS - Faktor 0,023 benötigt für die Umrechnung des Netzwerk-Verkehrs in GB/Jahr 
  #   sql.str<-paste("select load.app, max(decode(load.CTYPE,'Network_out',load.expval*tariff.value*0.023,NULL)) NWO_IAAS,", 
  # " max(decode(load.CTYPE,'Network_in',load.expval*tariff.value*0.023,NULL)) NWI_IAAS, max(decode(load.CTYPE,'Storage',load.expval*tariff.value,NULL)) STO_IAAS", 
  # " from load,tariff where load.sid=",as.character(SID.val)," and tariff.pid='",as.character(PROVIDER.val),"' and load.ctype=tariff.ctype group by load.app order by load.app asc",sep="")
  
  # max() funktion wird nur für decode() benötigt, keine Effekte auf Kosten
  PROVNWSTO.cost.df<-dbGetQuery(con,"select load.app, max(decode(load.CTYPE,'Network_out',load.expval*tariff.value*0.023,NULL)) NWO_IAAS, max(decode(load.CTYPE,'Network_in',load.expval*tariff.value*0.023,NULL)) NWI_IAAS, max(decode(load.CTYPE,'Storage',load.expval*tariff.value,NULL)) STO_IAAS from load,tariff where tariff.pid= :1  and ABS(load.qalpha- :2)<0.001 and ABS(load.qomega- :3)<0.001 and load.ctype=tariff.ctype group by load.app order by load.app asc",data=data.frame(PROVIDER=PROVIDER.val,QALPHA=ALPHA.val,QOMEGA=OMEGA.val))
  Names.PROVNWSTO.cost.df<-c("APP","COSTNWOIAAS","COSTNWIIAAS","COSTSTOIAAS")
  colnames(PROVNWSTO.cost.df)<-Names.PROVNWSTO.cost.df
  
  # Inhouse , Storage muss reserviert werden, also maximum abrechnen
#   sql.str<-paste("select load.app, max(decode(load.CTYPE,'Network_out',load.expval*tariff.value*0.023*szenario.pdelta,NULL)) NWO_IH,",
# " max(decode(load.CTYPE,'Network_in',load.expval*tariff.value*0.023*szenario.pdelta,NULL)) NWI_IH, max(decode(load.CTYPE,'Storage',load.qoval*tariff.value*szenario.pdelta,NULL)) STO_IH",
# " from load,tariff,szenario where load.sid=",as.character(SID.val)," and tariff.pid='",as.character(CLIENT.val),"' and load.ctype=tariff.ctype and load.sid=szenario.sid group by load.app order by load.app asc",sep="")
#   CLIENTNWSTO.cost.mat<-sqlQuery(channel,sql.str,stringsAsFactors=FALSE)
  CLIENTNWSTO.cost.df<-dbGetQuery(con,"select load.app, max(decode(load.CTYPE,'Network_out',load.expval*tariff.value*0.023,NULL)) NWO_IH, max(decode(load.CTYPE,'Network_in',load.expval*tariff.value*0.023,NULL)) NWI_IH, max(decode(load.CTYPE,'Storage',load.qoval*tariff.value,NULL)) STO_IH from load,tariff where tariff.pid= :1  and ABS(load.qalpha- :2)<0.001 and ABS(load.qomega- :3)<0.001 and load.ctype=tariff.ctype group by load.app order by load.app asc",data=data.frame(PROVIDER=CLIENT.val,QALPHA=ALPHA.val,QOMEGA=OMEGA.val))
  Names.CLIENTNWSTO.cost.df<-c("APP","COSTNWOIH","COSTNWIIH","COSTSTOIH")
  colnames(CLIENTNWSTO.cost.df)<-Names.CLIENTNWSTO.cost.df
   
  ## Basis-Berechnung Multiplikation mit den Kostenfaktoren PDELTA, ADELTA
  CLIENTINST.cost.df$COSTINSTIH<-CLIENTINST.cost.df$COSTINSTIH*PDELTA.val  
  CLIENTNWSTO.cost.df$COSTNWOIH<-CLIENTNWSTO.cost.df$COSTNWOIH*PDELTA.val
  CLIENTNWSTO.cost.df$COSTNWIIH<-CLIENTNWSTO.cost.df$COSTNWIIH*PDELTA.val
  CLIENTNWSTO.cost.df$COSTSTOIH<-CLIENTNWSTO.cost.df$COSTSTOIH*PDELTA.val
  head(CLIENTNWSTO.cost.df)

  ALPHAINST.cost.df$COSTAINSTIAAS<-ALPHAINST.cost.df$COSTAINSTIAAS*ADELTA.val  
  OMEGAINST.cost.df$COSTOINSTIAAS<-OMEGAINST.cost.df$COSTOINSTIAAS*ADELTA.val
  ONDEM.cost.df$COSTDYNINSTIAAS<-ONDEM.cost.df$COSTDYNINSTIAAS*ADELTA.val  
  
  PROVNWSTO.cost.df$COSTNWOIAAS<-PROVNWSTO.cost.df$COSTNWOIAAS*ADELTA.val
  PROVNWSTO.cost.df$COSTNWIIAAS<-PROVNWSTO.cost.df$COSTNWIIAAS*ADELTA.val
  PROVNWSTO.cost.df$COSTSTOIAAS<-PROVNWSTO.cost.df$COSTSTOIAAS*ADELTA.val
  head(PROVNWSTO.cost.df)
  

  ################################################################
  ## Zusammenfassung der Variabilitäten
  ##
  ################################################################
  # Variability
  #   sql.str<-paste("select load.app,max(decode(load.CTYPE,'Network_out',load.semivar ,NULL)) NWO, max(decode(load.CTYPE,'Network_in',load.semivar ,NULL)) NWI,",
  # " max(decode(load.CTYPE,'Storage',load.semivar ,NULL)) STO, max(decode(load.CTYPE,'CPU',load.semivar ,NULL)) CPU, max(decode(load.CTYPE,'RAM',load.semivar ,NULL)) RAM",
  # " from load where load.sid=",as.character(SID.val)," group by load.app order by load.app asc",sep="")

  SEMIVAR.df<-dbGetQuery(con,"select load.app,max(decode(load.CTYPE,'Network_out',load.semivar ,NULL)) NWO, max(decode(load.CTYPE,'Network_in',load.semivar ,NULL)) NWI, max(decode(load.CTYPE,'Storage',load.semivar ,NULL)) STO, max(decode(load.CTYPE,'CPU',load.semivar ,NULL)) CPU, max(decode(load.CTYPE,'RAM',load.semivar ,NULL)) RAM from load where Abs(load.qalpha- :1)<0.01 and Abs(load.qomega- :2)<0.001 group by load.app order by load.app asc",data=data.frame(QALPHA=ALPHA.val,QOMEGA=OMEGA.val))
  SEMIVAR.df<-data.frame(SEMIVAR.df,sapply(SEMIVAR.df$APP,lnorm,mData=SEMIVAR.df,iOrder=LNORM.val))
  colnames(SEMIVAR.df)<-c("APP","SVNWO","SVNWI","SVSTO","SVCPU","SVRAM","VARIAB")

  # Coeficient of Variation
  #   sql.str<-paste("select load.app,max(decode(load.CTYPE,'Network_out',load.COEFVAR ,NULL)) NWO, max(decode(load.CTYPE,'Network_in',load.COEFVAR ,NULL)) NWI,",
  # " max(decode(load.CTYPE,'Storage',load.COEFVAR ,NULL)) STO, max(decode(load.CTYPE,'CPU',load.COEFVAR ,NULL)) CPU, max(decode(load.CTYPE,'RAM',load.COEFVAR ,NULL)) RAM",
  # " from load where load.sid=",as.character(SID.val)," group by load.app order by load.app asc",sep="")
  COEFVAR.df<-dbGetQuery(con,"select load.app,max(decode(load.CTYPE,'Network_out',load.COEFVAR ,NULL)) NWO, max(decode(load.CTYPE,'Network_in',load.COEFVAR ,NULL)) NWI, max(decode(load.CTYPE,'Storage',load.COEFVAR ,NULL)) STO, max(decode(load.CTYPE,'CPU',load.COEFVAR ,NULL)) CPU, max(decode(load.CTYPE,'RAM',load.COEFVAR ,NULL)) RAM from load where abs(load.qalpha- :1)<0.001 and abs(load.qomega- :2)<0.001  group by load.app order by load.app asc", data=data.frame(QALPHA=ALPHA.val,QOMEGA=OMEGA.val))
  COEFVAR.df<-data.frame(COEFVAR.df,sapply(COEFVAR.df$APP,lnorm,mData=COEFVAR.df,iOrder=LNORM.val))      
  colnames(COEFVAR.df)<-c("APP","CVNWO","CVNWI","CVSTO","CVCPU","CVRAM","AGGCOEFVAR")
        
  
  ################################################################
  ## Zusammenfassung aller Daten
  ## mittels merge() siehe http://www.r-bloggers.com/joining-data-frames-in-r/
  ################################################################
  
#   ALPHAINST.vec$APP,
#   ALPHAINST.vec$INSTNR,
#   OMEGAINST.vec$INSTNR,
#   CLIENTINST.vec$INSTNR,
#   ALPHAINST.cost.vec,
#   OMEGAINST.cost.vec,
#   CLIENTINST.cost.vec,
#   ONDEM.cost.vec$ICOST,
#   PROVNWSTO.cost.mat$NWO_IAAS,
#   PROVNWSTO.cost.mat$NWI_IAAS,
#   PROVNWSTO.cost.mat$STO_IAAS,
#   CLIENTNWSTO.cost.mat$NWO_IH,
#   CLIENTNWSTO.cost.mat$NWI_IH,
#   CLIENTNWSTO.cost.mat$STO_IH,
#   VARIAB.vec,
#   AGGCOEFVAR.vec)
  
  ALLDATA.df<-merge(ALPHAINST.cost.df,OMEGAINST.cost.df,by="APP",all=TRUE)
  ALLDATA.df<-merge(ALLDATA.df,CLIENTINST.cost.df,by="APP",all=TRUE)
  ALLDATA.df<-merge(ALLDATA.df,ONDEM.cost.df,by="APP",all=TRUE)
  ALLDATA.df<-merge(ALLDATA.df,PROVNWSTO.cost.df,by="APP",all=TRUE)
  ALLDATA.df<-merge(ALLDATA.df,CLIENTNWSTO.cost.df,by="APP",all=TRUE)
  ALLDATA.df<-merge(ALLDATA.df,SEMIVAR.df[,c(1,7)],by="APP",all=TRUE)
  ALLDATA.df<-merge(ALLDATA.df,COEFVAR.df[,c(1,7)],by="APP",all=TRUE)
  

  EXPERIMDATA.df<-data.frame(EXPERIM=rep.int(c(experim.db[k,1]),length(dim(ALLDATA.df)[1])), SID=rep.int(c(SID.val),length(dim(ALLDATA.df)[1])),ALLDATA.df)

  #colnames(EXPERIM.df)<-c(c("EXPERIM","SID"),Names.ALPHAINST.cost.df,Names.OMEGAINST.cost.df,Names.CLIENTINST.cost.df,Names.PROVNWSTO.cost.df,
  #head(EXPERIM.df)

   # Berechne die Funktionen m_IaaS und m_Inhouse
  COSTIAAS<-EXPERIMDATA.df[,"COSTAINSTIAAS"]+EXPERIMDATA.df[,"COSTDYNINSTIAAS"]+EXPERIMDATA.df[,"COSTNWOIAAS"]+EXPERIMDATA.df[,"COSTNWIIAAS"]+EXPERIMDATA.df[,"COSTSTOIAAS"] #elastischer Tarif
  COSTIH<-EXPERIMDATA.df[,"COSTINSTIH"]+EXPERIMDATA.df[,"COSTNWOIH"]+EXPERIMDATA.df[,"COSTNWIIH"]+EXPERIMDATA.df[,"COSTSTOIH"] 
  COSTIAASRES<-EXPERIMDATA.df[,"COSTOINSTIAAS"]+EXPERIMDATA.df[,"COSTNWOIAAS"]+EXPERIMDATA.df[,"COSTNWIIAAS"]+EXPERIMDATA.df[,"COSTSTOIAAS"]
   
   #Fake Target column
  TARGET<-rep('NULL',dim(EXPERIMDATA.df)[1])
  TARGETRAND<-rep('NULL',dim(EXPERIMDATA.df)[1])
  
  EXPERIMDATA.all.df<-data.frame(EXPERIMDATA.df,COSTIAAS,COSTIH,COSTIAASRES,TARGET,TARGETRAND)
  head(EXPERIMDATA.all.df)
 
   rs <- dbSendQuery(con, "insert into OPTAPPRES (EXPERIM,SID,APP,NRAINSTIAAS,COSTAINSTIAAS,NROINSTIAAS,COSTOINSTIAAS,NRINSTIH,COSTINSTIH,COSTDYNINSTIAAS,COSTNWOIAAS,COSTNWIIAAS,COSTSTOIAAS,COSTNWOIH,COSTNWIIH,COSTSTOIH,VARIAB,AGGCOEFVAR,COSTIAAS,COSTIH, COSTIAASRES,TARGET,TARGETRAND) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23)",data = EXPERIMDATA.all.df)
   dbClearResult(rs)
 
   dbCommit(con)

} #for
  dbDisconnect(con)


