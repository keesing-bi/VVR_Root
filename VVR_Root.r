################################################################################################
############################################## VVR Root ########################################
############################################# By Jeffrey #######################################
################################################################################################
################################################################################################
############ This script is the root script to produce the VVR data for all countries###########
### Several functions are used, one for each country, to estimate the VVR for all countries ####
########### At the end of this script the VVR data of all contries are unioned together#########
####### The result of this script is a SQL table, the old table is automatically overwritten####
################################################################################################
################################################################################################


########################################### cleaning our environment ###########################

rm(list=ls())
gc()


############################################# KDK Estimates ####################################

source("VVR_KDK.r")

PSRDMonth_KDK <- VVR_KDK() 

source("VVR_KNL.r")

PSRDMonth_KNL <- VVR_KNL()

######################################### producing the union #################################
## not needed yet
## omdat we alleen nog maar KDK data hebben laat ik nu alleen KDK in


####################################### Creating the SQL table ###############################

## making sure our data frame had the appropiate format to make the SQL table

PSRDMonth_KDK[,sapply(PSRDMonth_KDK,is.character)] <- sapply(
  PSRDMonth_KDK[,sapply(PSRDMonth_KDK,is.character)],
  iconv,"WINDOWS-1252","UTF-8")

PSRDMonth_KNL[,sapply(PSRDMonth_KNL,is.character)] <- sapply(
  PSRDMonth_KNL[,sapply(PSRDMonth_KNL,is.character)],
  iconv,"WINDOWS-1252","UTF-8")

########################################### Union PSRD ###################################

PSRDMonth <-
            merge(PSRDMonth_KDK, PSRDMonth_KNL, all = TRUE)


## making a connection with the SQL server and database 
con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=jetenterprise;database=Nav13_DWH;trusted_connection= true;")

## Write the SQL table. If test == TRUE then is succeeded
test <- dbWriteTable(con = con, name = SQL("fVVR"), value = as.data.frame(PSRDMonth), overwrite = TRUE )

## breaking the connection with the SQL server
dbDisconnect(con)