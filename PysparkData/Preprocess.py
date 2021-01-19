from os import write
import sys 
import zipfile
import urllib.request
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import*
from pyspark import SparkContext , SparkConf
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, BooleanType
from pyspark.sql import  DataFrame
from pyspark.sql.functions import unix_timestamp, from_unixtime, expr, monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import to_date

print( "Télechargement et extraction du fichier StockUniteLegaleHistorique_utf8 ")
# Loader first file
fileZip1 = urllib.request.urlretrieve('https://files.data.gouv.fr/insee-sirene/StockUniteLegaleHistorique_utf8.zip', "Fichier1.zip")
file1 = zipfile.ZipFile("Fichier1.zip")
file1.extract('StockUniteLegaleHistorique_utf8.csv', './')
file1.close()
print( "Télechargement et extraction du fichier StockUniteLegaleHistorique_utf8 ")
# Loader second file
fileZip2 = urllib.request.urlretrieve('https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip', "Fichier2.zip")
file2 = zipfile.ZipFile("Fichier2.zip")
file2.extract('StockUniteLegale_utf8.csv', './')
file2.close()


config= SparkConf().setAppName("TeemLab").set("spark.jars", "postgresql-42.2.18.jar")
sc = SparkContext(conf=config)
ul_historique_data = sc.textFile("StockUniteLegaleHistorique_utf8.csv").map(lambda ligne: ligne.split(","))
ul_data= sc.textFile("StockUniteLegale_utf8.csv").map(lambda ligne: ligne.split(","))
sqlContext=SQLContext(sc)

#DataFrame
df_ul_historique_data_map=ul_historique_data.map(lambda p: Row(siren= p[0],dateFin= p[1],dateDebut = p[2],etatAdministratifUniteLegale = p[3],changementEtatAdministratifUniteLegale = p[4],nomUniteLegale = p[5],changementNomUniteLegale = p[6], nomUsageUniteLegale = p[7],changementNomUsageUniteLegale = p[8],denominationUniteLegale = p[9],changementDenominationUniteLegale = p[10],denominationUsuelle1UniteLegale = p[11],denominationUsuelle2UniteLegale = p[12],denominationUsuelle3UniteLegale = p[13],changementDenominationUsuelleUniteLegale = p[14],categorieJuridiqueUniteLegale = p[15],changementCategorieJuridiqueUniteLegale =  p[16],activitePrincipaleUniteLegale = p[17],nomenclatureActivitePrincipaleUniteLegale = p[18],changementActivitePrincipaleUniteLegale = p[19], nicSiegeUniteLegale =p[20], changementNicSiegeUniteLegale=p[21], economieSocialeSolidaireUniteLegale=p[22], changementEconomieSocialeSolidaireUniteLegale=p[23],caractereEmployeurUniteLegale=p[24], changementCaractereEmployeurUniteLegale=p[25]))
df_ul_historique_data=sqlContext.createDataFrame(df_ul_historique_data_map)
df_ul_historique_data.registerTempTable ("StockUniteLegaleHistorique")

df_ul_data_map=ul_data.map(lambda p: Row(siren= p[0],statutDiffusionUniteLegale= p[1],unitePurgeeUniteLegale = p[2],dateCreationUniteLegale = p[3],sigleUniteLegale = p[4],sexeUniteLegale = p[5],prenom1UniteLegale = p[6],prenom2UniteLegale = p[7],prenom3UniteLegale = p[8],prenom4UniteLegale = p[9],prenomUsuelUniteLegale = p[10],pseudonymeUniteLegale = p[11],identifiantAssociationUniteLegale = p[12],trancheEffectifsUniteLegale = p[13],anneeEffectifsUniteLegale = p[14],dateDernierTraitementUniteLegale = p[15],nombrePeriodesUniteLegale =  p[16],categorieEntreprise = p[17],anneeCategorieEntreprise = p[18],dateDebut = p[19], etatAdministratifUniteLegale =p[20], nomUniteLegale=p[21], nomUsageUniteLegale=p[22],  denominationUniteLegale=p[23],denominationUsuelle1UniteLegale=p[24], denominationUsuelle2UniteLegale=p[25],denominationUsuelle3UniteLegale=p[26], categorieJuridiqueUniteLegale=p[27], activitePrincipaleUniteLegale=p[28], nomenclatureActivitePrincipaleUniteLegale=p[29], nicSiegeUniteLegale=p[30], economieSocialeSolidaireUniteLegale=p[31], caractereEmployeurUniteLegale=p[32]))
df_ul_data=sqlContext.createDataFrame(df_ul_data_map)
df_ul_data.registerTempTable ("StockUniteLegale")

#-------------------------------------------------------------------------------------
# Preprocess ul_historique_data
df1_ul_historique_data=sqlContext.sql("select * from StockUniteLegaleHistorique")

#cast dateFin && dateDebut
df2_ul_historique_data = df1_ul_historique_data.withColumn('dateFin',to_date(df1_ul_historique_data.dateFin, 'yyyy-MM-dd')).withColumn('dateDebut',to_date(df1_ul_historique_data.dateDebut, 'yyyy-MM-dd'))

#cast boolean
df3_ul_historique_data=df2_ul_historique_data.withColumn('changementEtatAdministratifUniteLegale', col("changementEtatAdministratifUniteLegale").cast(BooleanType())).withColumn('changementNomUniteLegale', col("changementNomUniteLegale").cast(BooleanType())).withColumn('changementNomUsageUniteLegale', col("changementNomUsageUniteLegale").cast(BooleanType())).withColumn('changementDenominationUniteLegale', col("changementDenominationUniteLegale").cast(BooleanType())).withColumn('changementDenominationUsuelleUniteLegale', col("changementDenominationUsuelleUniteLegale").cast(BooleanType())).withColumn('changementCategorieJuridiqueUniteLegale', col("changementCategorieJuridiqueUniteLegale").cast(BooleanType())).withColumn('changementActivitePrincipaleUniteLegale', col("changementActivitePrincipaleUniteLegale").cast(BooleanType())).withColumn('changementNicSiegeUniteLegale', col("changementNicSiegeUniteLegale").cast(BooleanType())).withColumn('changementEconomieSocialeSolidaireUniteLegale', col("changementEconomieSocialeSolidaireUniteLegale").cast(BooleanType())).withColumn('changementCaractereEmployeurUniteLegale', col("changementCaractereEmployeurUniteLegale").cast(BooleanType()))

            #df3_ul_historique_data.printSchema()
            #df3_ul_historique_data.cache()
#-------------------------------------------------------------------------------------
# Preprocess ul_data
df1_ul_data=sqlContext.sql("select * from StockUniteLegale")

#cast dateFin && dateDebut
df2_ul_data = df1_ul_data.withColumn('dateCreationUniteLegale',to_date(df1_ul_data.dateCreationUniteLegale, 'yyyy-MM-dd')).withColumn('dateDebut',to_date(df1_ul_data.dateDebut, 'yyyy-MM-dd')).withColumn('dateDernierTraitementUniteLegale', to_date(df1_ul_data.dateDernierTraitementUniteLegale, 'yyyy-MM-dd'))

#cast boolean
df3_ul_data=df2_ul_data.withColumn('unitePurgeeUniteLegale', col("unitePurgeeUniteLegale").cast(BooleanType()))

                #df3_ul_data.printSchema()

                #df3_ul_data.cache()
#-------------------------------------------------------------------------------------
# Jointure without duplicated
df_j=sqlContext.sql("select sh.siren,sh.dateFin,sh.dateDebut,sh.etatAdministratifUniteLegale,sh.changementEtatAdministratifUniteLegale,sh.nomUniteLegale,sh.changementNomUniteLegale,sh.nomUsageUniteLegale,sh.changementNomUsageUniteLegale,sh.denominationUniteLegale,sh.changementDenominationUniteLegale,sh.denominationUsuelle1UniteLegale,sh.denominationUsuelle2UniteLegale,sh.denominationUsuelle3UniteLegale,sh.changementDenominationUsuelleUniteLegale,sh.categorieJuridiqueUniteLegale,sh.changementCategorieJuridiqueUniteLegale,sh.activitePrincipaleUniteLegale,sh.nomenclatureActivitePrincipaleUniteLegale,sh.changementActivitePrincipaleUniteLegale,sh.nicSiegeUniteLegale,sh.changementNicSiegeUniteLegale,sh.economieSocialeSolidaireUniteLegale,sh.changementEconomieSocialeSolidaireUniteLegale,sh.caractereEmployeurUniteLegale,sh.changementCaractereEmployeurUniteLegale,   statutDiffusionUniteLegale,unitePurgeeUniteLegale,dateCreationUniteLegale,sigleUniteLegale,sexeUniteLegale,prenom1UniteLegale,prenom2UniteLegale,prenom3UniteLegale,prenom4UniteLegale,prenomUsuelUniteLegale,pseudonymeUniteLegale,identifiantAssociationUniteLegale,trancheEffectifsUniteLegale,anneeEffectifsUniteLegale,dateDernierTraitementUniteLegale,nombrePeriodesUniteLegale,categorieEntreprise,anneeCategorieEntreprise from StockUniteLegaleHistorique as sh , StockUniteLegale as s where sh.siren = s.siren")
#Add Id
df_join = df_j.withColumn("id", monotonically_increasing_id())
#-------------------------------------------------------------------------------------
# upload to Postgres

# upload to Postgres
PSQL_SERVERNAME ="db"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME="postgresdb"
PSQL_USERNAME="postgres"
PSQL_PASSWORD="postgrespw"
TABLE = "unitelegale_unitelegale"
URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"


spark = SparkSession \
    .builder \
    .appName("Postgresdb") \
    .getOrCreate()

mode="overwrite"
properties=\
{
    "format" : "jdbc", \
    "url": URL, \
    "dbtable": TABLE,\
    "user": PSQL_USERNAME, \
    "password": PSQL_PASSWORD,\
    "driver": "org.postgresql.Driver"}
print("chargement de notre base de données")
df_join.write.jdbc(url=URL, table=TABLE, mode=mode, properties=properties)
print("chargement terminé")


