
import os
import numpy as np
import pandas as pd
from funkcje import data_processing, data_to_sql, find_newest_file_with_text_fragment, print_one

a= 0.6
path = r"D:\00 OPROGRAMOWANIE\05 INTEGRACJE\03_PROGNOZA_POGODY\KPEC_prognoza.csv"
pathParts = path.split("\\")

folderPath = r'D:\00 OPROGRAMOWANIE\05 INTEGRACJE\03_PROGNOZA_POGODY\00 ARCHIWUM'
filesCatalog = os.listdir(folderPath)


delimeter = "-"
filesData = [file.split(delimeter) for file in filesCatalog]
filesColumns = ['Nazwa','Dzien','Miesiac','Rok','Godzina','Minuta','Sekunda']

files = (pd.DataFrame(filesData, columns = filesColumns)
           .sort_values(by=['Rok','Miesiac','Dzien','Godzina','Minuta'], ascending = False)
           .iloc[:6,:]
           .assign(
               fileName = lambda df_: df_.Nazwa + delimeter + 
               df_.Dzien + delimeter + 
               df_.Miesiac  + delimeter + 
               df_.Rok + delimeter + 
               df_.Godzina + delimeter +
               df_.Minuta + delimeter + 
               df_.Sekunda
               )
           .iloc[:,[-1]]
           .values
           
           
          )
files = [item for sublist in files for item in sublist]



########################################3


#Smoothed ambient temperature
# dfTempSmoothedOld = (pd
#       .read_csv(path, 
#                   decimal = ",",
#                  skiprows = 1, 
#                  sep = ";")
#       .iloc[1:,:2]
#       .sort_values(by='Data Czas')
#       .astype({"Temperatura":"float64"})
#       .assign(
#           TagTime = lambda df_: pd.to_datetime(df_['Data Czas'])
#             )
#       .assign(
#           TagValue = lambda x: x['Temperatura'].expanding().apply(
#             lambda s: s.iloc[0] + (s.iloc[-1] - s.iloc[0]) * a if not s.empty else pd.Series(index=s.index),
#             raw=False),
#           TagName = "TemperatureAvgOld"
#             )
#       .iloc[:,-3:]
#      )

# #MinTs for critical points in Miasto/Fordon area
# dfMiastoFordonOld = (dfTempSmoothedOld
#       .loc[:,['TagTime','TagValue']]
#       .assign(
#           TminWezly = lambda df_: [(value * (-2.5133) + 83.596) if value < 7 else 65.8 for value in df_.TagValue],
#           TagName = 'TempMinNodesMiastoOld'
#                  )
#        .loc[:,['TagTime','TminWezly','TagName']]
#        .rename(columns = {'TminWezly' : 'TagValue'})
#                  )


# #MinTs for critical points in OsowaGora area
# dfOsowaOld = (dfTempSmoothedOld
#       .loc[:,['TagTime','TagValue']]
#       .assign(
#           TminWezly = lambda df_: [(value * (-2.2364) + 77.197) if value < 6 else 65.0 for value in df_.TagValue],
#           TagName = 'TempMinNodesOsowaOld'
#                  )
#       .loc[:,['TagTime','TminWezly','TagName']]
#       .rename(columns = {'TminWezly' : 'TagValue'})
#             )



#################################################################3



#Temp basic
dfTempBasic = (pd
      .read_csv(os.path.join(folderPath,files[0]), 
                  decimal = ",",
                 skiprows = 1, 
                 sep = ";")
      .iloc[1:,:2]
      .rename(columns = {'DataCzas' : 'Data Czas'})
      .sort_values(by='Data Czas')
      .astype({"Temperatura":"float64"})
      .assign(
          TagTime = lambda df_: pd.to_datetime(df_['Data Czas'])
            )
      .assign(
          TagValue = lambda x: x['Temperatura'],
          TagName = "Temperature"
            )
      .iloc[:,-3:]
     )

#MinTS for ECII and ZTPOK Plant

dfTsECII = (dfTempBasic
          .loc[:,['TagTime','TagValue']]
          .assign(
              TminZrodloECII = lambda df_: [(value * (-2.5133) + 85.996) if value < 7 else 68.2 for value in df_.TagValue],
              TagName = 'TempMinPlantEC2ZTPOK'
                 )
            .iloc[:,[0,2,3]]
            .rename(columns = {'TminZrodloECII' : 'TagValue'})
          
          )


#MinTS for ECI Plant

dfTsECI = (dfTempBasic
          .loc[:,['TagTime','TagValue']]
          .assign(
              TminZrodloECI = lambda df_: [(value * (-2.4959) + 84.723) if value < 7 else 68 for value in df_.TagValue],
              TagName = 'TempMinPlantECI'
                 )
            .iloc[:,[0,2,3]]
            .rename(columns = {'TminZrodloECI' : 'TagValue'})
          
          )

#MinTS for OsowaGora Plant

dfTsOsowaGora = (dfTempBasic
          .loc[:,['TagTime','TagValue']]
          .assign(
              TminZrodloOsowaGora = lambda df_: [(value * (-2.2395) + 80.181) if value < 6 else 68 for value in df_.TagValue],
              TagName = 'TempMinPlantOsowaGora'
                 )
            .iloc[:,[0,2,3]]
            .rename(columns = {'TminZrodloOsowaGora' : 'TagValue'})
          
          )




#Loading forecast data
merged_df = (pd.read_csv(os.path.join(folderPath,files[0]),
                         decimal = ",",
                 skiprows = 1, 
                 sep = ";")
              .iloc[:,:2]
              .rename(columns = {'DataCzas' : 'Data Czas'})
              .sort_values(by='Data Czas')
              .astype({"Temperatura":"float64"})
              .assign(
                  TagTime = lambda df_: pd.to_datetime(df_['Data Czas'])
                            )
              .loc[:,['TagTime','Temperatura']]
                )


for idx, file in enumerate(files[1:5]):
    
    
    dfTemp = (pd.read_csv(os.path.join(folderPath,file),
                         decimal = ",",
                 skiprows = 1, 
                 sep = ";")
              .iloc[:,:2]
              .rename(columns = {'DataCzas' : 'Data Czas'})
              .sort_values(by='Data Czas')
              .astype({"Temperatura":"float64"})
              .assign(
                  TagTime = lambda df_: pd.to_datetime(df_['Data Czas'])
                            )
              .loc[:,['TagTime','Temperatura']]
              .rename(columns = {'Temperatura':f'Temperatura_{idx}'})
                )
    

    merged_df = (pd.merge(merged_df, dfTemp, on='TagTime', how='outer')
                 )
    

#Smoothed ambient temperature
dfTempSmoothed = (merged_df
             .sort_values(by = 'TagTime')
             .assign(
                 TagValue = lambda df_: df_.filter(like='Temperatura').mean(axis=1),
                 TagName = 'TemperatureAvg'
                     )
             .iloc[:,[0,-2,-1]]
                 )

dfTempSmoothedSave = (merged_df
             .sort_values(by = 'TagTime')
             .assign(
                 TagValue = lambda df_: df_.filter(like='Temperatura').mean(axis=1),
                 TagName = 'TemperatureAvg'
                     )
             .iloc[:,[0,-2,-1]]
            .iloc[np.where(merged_df.TagTime >= dfTempBasic.TagTime.min())]
                 )


#MinTs for critical points in Miasto/Fordon area
dfMiastoFordon = (dfTempSmoothed
            .assign(

                TminWezly_SredniePrognozy = lambda df_: [(value * (-2.5133) + 83.596) if value < 7 else 65.8 for value in df_.TagValue]
                    )
            .assign(
                TempSredniaWygladzona = lambda x: x['TagValue'].expanding().apply(
                        lambda s: s.iloc[0] + (s.iloc[-1] - s.iloc[0]) * a if not s.empty else pd.Series(index=s.index),
                        raw=False)
                    )
             .assign(
                 TminWezly_SredniePrognozyWygladzone = lambda df_: [(value * (-2.5133) + 83.596) if value < 7 else 65.8 for value in df_.TempSredniaWygladzona]
                    )
             .assign(
                 TagValue = lambda df_: 
                     df_.TminWezly_SredniePrognozyWygladzone - 
                 (df_.TminWezly_SredniePrognozyWygladzone.mean() - df_.TminWezly_SredniePrognozy.mean()),
                 TagName = 'TempMinNodesMiasto'
             )
 
             .sort_index(axis = 1)
             .loc[:,['TagTime','TagValue','TagName']]
            .iloc[np.where(dfTempSmoothed.TagTime >= dfTempBasic.TagTime.min())]
            )



#MinTs for critical points in OsowaGora area
dfOsowa = (dfTempSmoothed
            .assign(

                TminWezly_SredniePrognozy = lambda df_: [(value * (-2.2364) + 77.197) if value < 6 else 65.0 for value in df_.TagValue]
                    )
            .assign(
                TempSredniaWygladzona = lambda x: x['TagValue'].expanding().apply(
                        lambda s: s.iloc[0] + (s.iloc[-1] - s.iloc[0]) * a if not s.empty else pd.Series(index=s.index),
                        raw=False)
                    )
             .assign(
                 TminWezly_SredniePrognozyWygladzone = lambda df_: [(value * (-2.2364) + 77.197) if value < 6 else 65.0 for value in df_.TempSredniaWygladzona]
                    )
             .assign(
                 TagValue = lambda df_: 
                     df_.TminWezly_SredniePrognozyWygladzone - 
                 (df_.TminWezly_SredniePrognozyWygladzone.mean() - df_.TminWezly_SredniePrognozy.mean()),
                 TagName = 'TempMinNodesOsowa'
             )
 
         .sort_index(axis = 1)
         .loc[:,['TagTime','TagValue','TagName']]
        .iloc[np.where(dfTempSmoothed.TagTime >= dfTempBasic.TagTime.min())]
            )

#TsMin for nodes in Osowa for basic ambient temperature forecast
dfTempMinNodesOsowaBasic = (dfTempBasic
    .assign(
        TempMinNodesOsowaBasic = lambda df_: [(value * (-2.2364) + 77.197) if value < 6 else 65.0 for value in df_.TagValue],
#       TempMinNodesMiastoBasic = lambda df_: [(value * (-2.5133) + 83.596) if value < 7 else 65.8 for value in df_.TagValue]
        TagName = 'TempMinNodesOsowaBasic'
          )
 [['TagTime','TempMinNodesOsowaBasic','TagName']]
    .rename(columns = {'TempMinNodesOsowaBasic':'TagValue'})                         
                        )


#TsMin for nodes in Miasto for basic ambient temperature forecast
dfTempMinNodesMiastoBasic = (dfTempBasic
    .assign(
#         TempMinNodesOsowaBasic = lambda df_: [(value * (-2.2364) + 77.197) if value < 6 else 65.0 for value in df_.TagValue],
       TempMinNodesMiastoBasic = lambda df_: [(value * (-2.5133) + 83.596) if value < 7 else 65.8 for value in df_.TagValue],
        TagName = 'TempMinNodesMiastoBasic'
          )
 [['TagTime','TempMinNodesMiastoBasic','TagName']]
    .rename(columns = {'TempMinNodesMiastoBasic':'TagValue'}) 
                            )


#Concatenation of above dataframes
df = pd.concat([
    dfMiastoFordon
    ,dfTempSmoothedSave
    ,dfOsowa
#     ,dfTempBasic 
    ,dfTsECII
    ,dfTsECI
    ,dfTsOsowaGora
#     ,dfTempSmoothedOld
#      ,dfMiastoFordonOld
#     ,dfOsowaOld
    ,dfTempMinNodesMiastoBasic
    ,dfTempMinNodesOsowaBasic
    
                ]
                )


#Loading data into SQL
[server_name, database_name, table_name] = ['KPEC-KELVIN','BAZA_INTEGRACYJNA','tbl_Temperatura_Wygladzona']
data_to_sql(server_name, database_name, table_name, df)
