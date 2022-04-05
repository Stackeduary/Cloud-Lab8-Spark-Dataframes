import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkFun
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors
import matplotlib
import pandas

if __name__ == "__main__":

    #check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: Exercises-8_3-and-8_4.py <input folder> ")
        exit(-1)

    #Set a name for the application
    appName = "DataFrame Example"

    #Set the input folder location to the first argument of the application
    #NB! sys.argv[0] is the path/name of the script file
    input_folder = sys.argv[1]

    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    #read in the CSV dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    #header option forces Spark to automatically fetch column names from the first line in the dataset files
    dataset = spark.read \
                  .option("inferSchema", True) \
                  .option("header", True) \
                  .csv(input_folder)


    #Show 10 rows without truncating lines.
    #review content might be a multi-line string.
    # dataset.show(10, False)

    #Show dataset schema/structure with filed names and types
    dataset.printSchema()


    ##### Ex. 8.3

    # result = dataset.groupBy("Station Name").agg(sparkFun.avg("Humidity"), sparkFun.max("Humidity"))
    resultEx83 = dataset.filter("`Air Temperature` > 20").groupBy("Station Name").agg(sparkFun.avg("Humidity"))
    resultEx83.show()

    # result.write.format("csv").option("header", True).option("compression", "gzip").save("output")
    resultEx83.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save("output")



    ##### Ex. 8.4

    resultEx84 = dataset.select("Station Name", "`Solar Radiation`", sparkFun.substring("`Measurement Timestamp`", 1, 10).alias("day")) \
        .groupBy("Station Name", "day") \
        .agg(sparkFun.avg("`Solar Radiation`").alias("Avg Solar Radiation"), sparkFun.min("`Solar Radiation`").alias("Min Solar Radiation"), sparkFun.max("`Solar Radiation`").alias("Max Solar Radiation")) \
            .orderBy("Avg Solar Radiation", ascending=False)
    resultEx84.show()
    resultEx84.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save("output")

    #Stop Spark session
    #spark.stop()