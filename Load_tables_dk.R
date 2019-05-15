#######################################################################################################
############################################# Loading tables KDK ######################################
################################################### Jeffrey ###########################################
#######################################################################################################
#######################################################################################################
#######################################################################################################
################## this function is meant to load in the data tables for the VVR KDK estimate script ##
#######################################################################################################
#######################################################################################################
#######################################################################################################

########################################### if needed install packaged ################################

# install.packages("dplyr")
# install.packages("odbc")
# install.packages("DBI")

############################################# loading in libraries ####################################

LoadDateTables_DK <- function(table) {
  
  library(dplyr)
  library(odbc)
  library(DBI)
  
  print(paste("Loading the data table ", table ))
  
  ## making connection with the SQL server and database
  con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=jetenterprise;database=Nav13_DWH;trusted_connection= true;")
  
  ## loading in the fDistribution data via an SQL querie
  if (table == 'fDistribution'){
    Load_table <- dbGetQuery(con, "
                             SELECT 
                             [DW_Product_id]
                             ,[DW_Product_id_unknown]
                             ,sum(cast([Backorder] as int)) as [Backorder]
                             ,sum(cast([Coverage] as int)) as [Coverage]
                             ,sum(cast([Distributed] as int)) as [Distributed]
                             ,sum(cast([Return] as int)) as [Return]
                             ,sum(cast([Sales Volume] as int)) as [Sales Volume]
                             FROM [NAV13_DWH].[dbo].[fDistributorSales_DK]
                             group by		
                             [DW_Product_id]
                             ,[DW_Product_id_unknown]
                             
                             
                             "
    ) ## loading in the dProduct table
  } else if (table == 'dProduct'){
    Load_table <- dbGetQuery(con, "
                           select
                             *,
                             cast([Purchase Date] as date) as [Order Date]
                             from dProduct
                             where  Country = 'DK' --[Source] = 'EAN 18'
                             --and Country = 'DK'
                             -- and [Available for Circulation] = 1
                             --and [Edition code] not like '%G1'
                             --and [Edition code] not like '%G2'
                             --and [Edition code] not like '%G3'
                             --and [Edition code] not like '%G4'
                             "
    )
  } else if (table == 'dPO') {
    Load_table <- dbGetQuery(con, "
                             select
aa.[Edition Code],
                             aa.PO,
                             case when aa.ontvangsten is null then 0 else aa.ontvangsten end as ontvangsten,
                             case when aa.invoiced is null then 0 else aa.invoiced end as invoiced,
                             case when aa.onverkochten is null then 0 else aa.onverkochten end as onverkochten
                             from(
                             select 
                             --i.Description as Titelnaam, 
                             S.[Edition Code],
                             --s.[Item No.],
                             --s.[Cover No.],
                             --i.DW_Account,
                             --i.[Status Code],
                             -- cast(d.[date value] as date) as PSRD,
                             (
                             SELECT cast(sum(Quantity) as int)
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Line] sal
                             WHERE ((sal.DW_Account like '%DEN%')) AND sal.[Shortcut Dimension 1 Code]=S.[Edition Code] 
                             ) as PO,
                             (
                             SELECT cast(sum(Quantity) as int)
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Shipment Line] ssl
                             WHERE ((ssl.DW_Account like '%DEN%')) and ssl.[Shortcut Dimension 1 Code]=S.[Edition Code]
                             ) as ontvangsten,
                             (
                             SELECT cast(sum(Quantity) as int)--, (sil.[Shortcut Dimension 1 Code])
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Invoice Line] sil
                             WHERE ((sil.DW_Account like '%DEN%'))  AND sil.[Shortcut Dimension 1 Code]=S.[Edition Code]
                             --     group by (sil.[Shortcut Dimension 1 Code])
                             ) as invoiced,
                             (
                             SELECT cast(sum(Quantity) as int)
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Cr.Memo Line] scl
                             WHERE ((scl.DW_Account like '%DEN%')) AND scl.[Shortcut Dimension 1 Code]=S.[Edition Code]
                             ) as onverkochten
                             --     ,s.*, '|', d.*
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Stockkeeping Unit] s
                             join [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Item] i on i.[No.] =s.[Item No.]
                             join [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_SKU Planning Date] d on s.[Item No.]=d.[Item No.] and s.[Variant Code]=d.[Variant Code]
                             where [Date Code] = 'PSRD' AND ([Date Value] between DATEADD(yy, -1, getdate()) and EOMONTH(getdate())) 
                             and i.[Status Code] in ('INT','KDK') and s.DW_Account like '%DEN%' --and (i.DW_Account like '%NED%' or i.DW_Account like '%SAN%') and (d.DW_Account like '%NED%' or d.DW_Account like '%SAN%') 
                             --and (s.DW_Account like '%NED%' or s.DW_Account like '%SAN%') t like '%DEN%'))
                             )aa
                             where aa.PO is not null
                             ")
  }
  
  
  ## SQL server disconect
  dbDisconnect(con)
  
  ## the data that is returned by this function
  return(Load_table)
  
  }


