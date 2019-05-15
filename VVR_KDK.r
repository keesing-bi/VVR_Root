##########################################################################################################################
############################################# VVR estimates KDK ##########################################################
################################################ By Jeffrey ##############################################################
##########################################################################################################################


########################################### if needed install packaged ###################################################

# install.packages("dplyr")
# install.packages("repr")
# install.packages("ggplot2")
# install.packages("odbc")
# install.packages("DBI")
# install.packages("lubridate")


VVR_KDK <- function() {


############################################### loading in libraries #####################################################

library(dplyr)
library(repr)
library(ggplot2)
library(odbc)
library(DBI)
library(lubridate)

############################################## loading in data tables ####################################################

source("Load_tables_dk.R")

fDistribution_aggr <- LoadDateTables_DK(table = 'fDistribution') 

dProduct <- LoadDateTables_DK(table = 'dProduct')

dPO <- LoadDateTables_DK(table = 'dPO')


############################################# calculating historical returns #############################################

## join between dproduct en fdistribution
Merge_dProd_fDist_all <- 
  merge.data.frame( dProduct, fDistribution_aggr, by.x = "DW_Id", by.y ="DW_Product_id")
Merge_dProd_fDist <- 
  filter(Merge_dProd_fDist_all, Closed == 'J') ## ALLES IS CLOSED?

## selecting the relevent data to calculate the historic return rate
HReturnsSelection <- 
  subset.data.frame(Merge_dProd_fDist, select = c(`Product Code`, Distributed, Return))

## calculating the global historic return rate (is calculated over all titels)
HReturnsGlobal <- 
  sum(HReturnsSelection %>% select(Return)) / sum(HReturnsSelection %>% select(Distributed)) 

## aggregating the returns over titel
HRetunsTitelReturn <- 
  aggregate.data.frame(list(returns=HReturnsSelection$Return), by=list(ProductCode=HReturnsSelection$`Product Code`), FUN = sum)

## aggregating the Distributed over titel
HRetunsTitelDist <- 
  aggregate.data.frame(list(Dist=HReturnsSelection$Distributed), by=list(ProductCode=HReturnsSelection$`Product Code`), FUN = sum)

## merging the above two aggregates
HReturnsTitle <-
  merge.data.frame(HRetunsTitelDist, HRetunsTitelReturn, by.x = "ProductCode", by.y = "ProductCode", sort = TRUE)

## calculating historical return rate for a titel
HReturnsTitle <-
  within.data.frame(HReturnsTitle, PercRetunTitle <- returns/Dist)

## join on historic return values for the titels with the root table
PSRDEditionHReturn <- 
  merge.data.frame(Merge_dProd_fDist_all, HReturnsTitle, by.x = "Product Code", by.y = "ProductCode", all.x = TRUE)

## deliting dist and returns because it is redundant
PSRDEditionHReturn  <-
  select(PSRDEditionHReturn, -c(Dist, returns))

###################### calculating estimated returns and sales ############################################################

## join met PO uit Navision
PSRDEditionHReturn <- 
  merge.data.frame (PSRDEditionHReturn, dPO, by.x = "Edition code", by.y = "Edition Code")

## making a new column containing ones
PSRDEditionHReturn$EstimatedReturn  <-
  rep(1, nrow(PSRDEditionHReturn))

## calculating estimated returns
for (i in 1:nrow(PSRDEditionHReturn)) {
  PSRDEditionHReturn$EstimatedReturn[i] <-
    if (PSRDEditionHReturn$`Cover No`[i] <= 5) {
      round(PSRDEditionHReturn$PO[i] * 0.7)
    } else if (PSRDEditionHReturn$`Cover No`[i] <= 10) {
      round(PSRDEditionHReturn$PO[i] * HReturnsGlobal)
    } else if ((PSRDEditionHReturn$PO[i]*PSRDEditionHReturn$PercRetunTitle[i]) >= PSRDEditionHReturn$onverkochten[i]) {
      round(PSRDEditionHReturn$PO[i]*PSRDEditionHReturn$PercRetunTitle[i])
    } else {
      round(PSRDEditionHReturn$EstimatedReturn[i] <-PSRDEditionHReturn$onverkochten[i])
    }
}

## calculating estimated sales
PSRDEditionHReturn <-
  within.data.frame(PSRDEditionHReturn, EstimatedSales <- PO - EstimatedReturn)


## selecting the relevant columns 
PSRDEditionHReturn_subset <- 
  subset.data.frame(PSRDEditionHReturn, select = c(`Edition code`, Distributed, PO, Return, `Sales Volume` ,PercRetunTitle, round(EstimatedReturn), EstimatedSales))

## merge with all the dproduct data for this country
Merge_All_dProd_fDist <- 
  merge.data.frame( dProduct, PSRDEditionHReturn_subset, by.x = "Edition code", by.y ="Edition code", all.x = TRUE)

# mocht je het ooit willen filteren voor de huidige maand gebruik dan onderstaande statement en comment die daar onder weer weg (die fitlerf voor het huidige jaar).
#PSRDMonth <- 
# filter(Merge_All_dProd_fDist, month(Merge_All_dProd_fDist$`Preliminary Sales Reporting Date (PSRD)`) == month(Sys.Date()) & year(Merge_All_dProd_fDist$`Preliminary Sales Reporting Date (PSRD)`) == year(Sys.Date()))

## filter for current year
PSRDMonth <- 
  filter(Merge_All_dProd_fDist, year(Merge_All_dProd_fDist$`Preliminary Sales Reporting Date (PSRD)`) == year(Sys.Date()))


# SQL tabel vullen

# controleren via sql query of table al bestaat anders leeg trekken

library(odbc)
library(DBI)

## filter for only the relevant data columns
PSRDMonth <- 
  subset.data.frame(PSRDMonth, select = c(DW_Id ,`Edition code`, `Product Title` ,EAN_20, PO, Return, `Sales Volume`, EstimatedReturn, EstimatedSales ))


## return VVR data
return(PSRDMonth)

}
