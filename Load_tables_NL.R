#######################################################################################################
############################################# Loading tables KNL ######################################
################################################### Jeffrey ###########################################
#######################################################################################################
#######################################################################################################
#######################################################################################################
######### this function is meant to load in the data tables for the VVR KDK estimate script ###########
#######################################################################################################
#######################################################################################################
#######################################################################################################

########################################### if needed install packaged ################################

# install.packages("dplyr")
# install.packages("odbc")
# install.packages("DBI")

############################################# loading in libraries ####################################

LoadDataTables_KNL <- function(table) {
  
  library(dplyr)
  library(odbc)
  library(DBI)
  
  print(paste("Loading the data table ", table ))
  
  ## making connection with the SQL server and database
  con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=jetenterprise;database=Nav13_DWH;trusted_connection= true;")
  
  ## loading in the fDistribution data via an SQL querie
  if (table == 'fDistribution'){
    Load_table <- dbGetQuery(con, "
                             select
	                           m.DW_Product_id,
                             sum(m.[Distributed]) as [Distributed],
                             sum(m.[Return]) as [Return],
                             sum(m.[Sales Volume]) as [Sales Volume]
                             from(
                             SELECT 
                             f.*,
                             p.Segment,
                             p.[Cover No],
                             p.[Edition code],
                             p.[Product Code],
                             p.[Product Title], 
                             p.[Preliminary Sales Reporting Date (PSRD)],
                             p.Merk,
                             p.Country
                             from(
                             select
                             fact.[DW_Id]
                             --      ,[EAN]
                             ,case when [DW_Product_id] is null then [DW_Product_id_unknown] else [DW_Product_id] end as [DW_Product_id] 
                             ,fact.[Distributor]
                             
                             ,cast([Distributed] as int) as [Distributed]
                             
                             ,cast([Return] as int) as [Return]
                             ,cast([Sales Volume] as int) as [Sales Volume]
                             
                             
                             ,case when dim.[Preliminary Sales Reporting Date (PSRD)] is null or [Preliminary Sales Reporting Date (PSRD)] = '' then '01-01-1900' else dim.[Preliminary Sales Reporting Date (PSRD)] end as [Sales date (PSRD)]
                             ,[Sourcefile]
                             ,Closed
                             
                             
                             
                             FROM 
                             ( 
                             SELECT * FROM  [NAV13_DWH].[dbo].[fDistributorSales]
                             union
                             SELECT * FROM  [NAV13_DWH].[dbo].[fDistributorSales_BE]
                             union
                             SELECT * FROM  [NAV13_DWH].[dbo].[fDistributorSales_SE]
                             )fact 
                             left join [NAV13_DWH].[dbo].[dProduct] dim 
                             on fact.DW_Product_id = Dim.DW_Id
                             left join [NAV13_DWH].[dbo].[Dist_Cost_perc] Cost 
                             on  fact.Distributor = Cost.[Distributor]
                             and year(dim.[Preliminary Sales Reporting Date (PSRD)]) = cast(Cost.[Year] as int)
                             where year(dim.[Last Sales Date (LSD)]) >= 2016 
                             and year(dim.[Preliminary Sales Reporting Date (PSRD)])  >= 2016
                             ) f
                             left join dbo.dProduct p on f.[DW_Product_id] = p.[DW_Id]
                             where p.[Preliminary Sales Reporting Date (PSRD)] between  DATEADD(yy, -1, getdate()) and EOMONTH(getdate())
                             and p.[Available for Circulation] = 1
                             and (f.Distributor = 'AldiPress'or f.Distributor = 'Betapress') 
                             and (p.[Edition code] not like '%G1' and p.[Edition code] not like '%G2' and p.[Edition code] not like '%G3' and p.[Edition code] not like '%G4')
                             ) m
                             where Country = 'NL' 
                             group by 
                             m.DW_Product_id
                             
                             "
    ) ## loading in the dProduct table
  } else if (table == 'dProduct'){
    Load_table <- dbGetQuery(con, "
                             select
                             *,
                             cast([Purchase Date] as date) as [Order Date]
                             from dProduct
                             where Country = 'NL'
                             -- and [Available for Circulation] = 1
                             --and [Edition code] not like '%G1'
                             --and [Edition code] not like '%G2'
                             --and [Edition code] not like '%G3'
                             --and [Edition code] not like '%G4'
                             "
    )
  }    else if (table == 'dPO') {
    Load_table <- dbGetQuery(con, "
                            
                             select
                             aa.*
                             from(
                             select 
                             i.Description as Titelnaam, 
                             S.[Edition Code],
                             s.[Item No.],
                             s.[Cover No.],
                             i.DW_Account,
                             i.[Status Code],
                             cast(d.[date value] as date) as PSRD,
                             (
                             SELECT cast(sum(Quantity) as int)
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Line] sal
                             WHERE ((sal.DW_Account ='Keesing Nederland BV' and sal.[Sell-to Customer No.]='K00132' and ([Address Code] = 'MED-01' or [Address Code] = 'ADP-13' or [Address Code] = 'SYNER-01' )) or (sal.DW_Account = 'M. Sanders Puzzelhobby B.V.' and sal.[Sell-to Customer No.]='105000'and  [Document Type] <> 3  and( [Address Code] = 'BTP-01' or [Address Code] = 'AWA-01'))) AND sal.[Shortcut Dimension 1 Code]=S.[Edition Code] 
                             ) as PO,
                             (
                             SELECT cast(sum(Quantity) as int)
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Shipment Line] ssl
                             WHERE ((ssl.DW_Account ='Keesing Nederland BV' and ssl.[Sell-to Customer No.]='K00132'  and ([Address Code] = 'MED-01' or [Address Code] = 'ADP-13' or [Address Code] = 'SYNER-01' )) or (ssl.DW_Account = 'M. Sanders Puzzelhobby B.V.' and  ssl.[Sell-to Customer No.]='105000' and( [Address Code] = 'BTP-01' or [Address Code] = 'AWA-01'))) AND ssl.[Shortcut Dimension 1 Code]=S.[Edition Code]
                             ) as ontvangsten,
                             (
                             SELECT cast(sum(Quantity) as int)--, (sil.[Shortcut Dimension 1 Code])
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Invoice Line] sil
                             WHERE ((sil.DW_Account ='Keesing Nederland BV' and sil.[Sell-to Customer No.]='K00132' and ([Address Code] = 'MED-01' or [Address Code] = 'ADP-13' or [Address Code] = 'SYNER-01' )) or (sil.DW_Account = 'M. Sanders Puzzelhobby B.V.' and sil.[Sell-to Customer No.]='105000' and( [Address Code] = 'BTP-01' or [Address Code] = 'AWA-01') )) AND sil.[Shortcut Dimension 1 Code]=S.[Edition Code]
                             --     group by (sil.[Shortcut Dimension 1 Code])
                             ) as invoiced,
                             (
                             SELECT cast(sum(Quantity) as int)
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Sales Cr.Memo Line] scl
                             WHERE ((scl.DW_Account ='Keesing Nederland BV' and scl.[Sell-to Customer No.]  ='K00132'  ) or (scl.DW_Account = 'M. Sanders Puzzelhobby B.V.' and  scl.[Sell-to Customer No.]='105000' )) and scl.Type=2 AND scl.[Shortcut Dimension 1 Code]=S.[Edition Code]
                             ) as onverkochten
                             --     ,s.*, '|', d.*
                             FROM [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Stockkeeping Unit] s
                             join [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_Item] i on i.[No.] =s.[Item No.]
                             join [ProdFin01Stage].[dbo].[NAV09_KNL_dbo_SKU Planning Date] d on s.[Item No.]=d.[Item No.] and s.[Variant Code]=d.[Variant Code]
                             where [Date Code] = 'PSRD' AND ([Date Value] between DATEADD(yy, -1, getdate()) and EOMONTH(getdate())) 
                             and i.[Status Code] in ('INT','KNL','SAN') and (i.DW_Account like '%NED%' or i.DW_Account like '%SAN%') and (d.DW_Account like '%NED%' or d.DW_Account like '%SAN%') 
                             and (s.DW_Account like '%NED%' or s.DW_Account like '%SAN%')
                             )aa
                             where aa.PO is not null and aa.PO <> 0 
                             ")
  }
  
  
  ## SQL server disconect
  dbDisconnect(con)
  
  ## the data that is returned by this function
  return(Load_table)
  
  }


