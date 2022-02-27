from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def setGlobalSparkSession(appName):
    global spark
    spark = SparkSession.builder.appName(appName).getOrCreate()

def readDataFrameFromText(file):
    return spark.read.text(file)
    
def verwerkDataFrame(df):
    return df.withColumn("value", F.upper(F.col("value")))
