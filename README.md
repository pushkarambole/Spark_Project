# Spark Project

## Description:
Developed Py-Spark solutions for efficiently performing the analysis of huge datasets using Scala and Python.

## Steps for executing the project along with inline outputs:

### I. Finding the hottest and coldest day along the station code and date for each year

>>> file_path = "/data/weather/2010"  
>>> inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2011"                                            
>>> inTextData2 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
19/11/29 16:00:50 WARN SharedInMemoryCache: Evicting cached table partition metadata from memory due to size constraints (spark.sql.hive.filesourcePartitionFileCacheSize = 262144000 bytes). This may impact query planning performance.
>>> inTextData2 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2012"                                            
>>> inTextData3 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2013"                                            
>>> inTextData4 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2014"                                            
>>> inTextData5 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
>>> data20102014=inTextData.unionAll(inTextData2).unionAll(inTextData3).unionAll(inTextData4).unionAll(inTextData5)  
>>> data20102014.count()  
18547460                                                                        
>>> rdd1 = data20102014.rdd  
>>> rdd2 = rdd1.map(lambda x: str(x).split('=')[1])  
>>> rdd3 = rdd2.map(lambda x: ' '.join(x.split()))  
>>> rdd4 = rdd3.map(lambda x: x[2:-2])  
>>> rdd4.saveAsTextFile('home/amboleps/spark20102014'+'temp')  
>>> newInData = spark.read.csv('home/amboleps/spark20102014'+'temp',header=False,sep=' ')  
>>> cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')  
>>> cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
...                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
...                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
...                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
...                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
...                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
...                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
...                     .withColumnRenamed('_c21','FRSHTT')  
>>> cleanData.show(2, False)  
+------+--------+----+------+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
|STN   |YEARMODA|TEMP|DEWP  |SLP   |STP   |VISIB|WDSP|MXSPD|GUST |MAX  |MIN  |PRCP |SNDP |FRSHTT|
+------+--------+----+------+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
|996400|20100101|57.2|9999.9|1024.7|9999.9|999.9|3.8 |7.8  |999.9|58.5*|56.1*|0.00I|999.9|000000|
|996400|20100102|60.0|9999.9|1019.0|9999.9|999.9|5.0 |13.6 |999.9|63.0*|57.7*|0.00I|999.9|000000|
+------+--------+----+------+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
only showing top 2 rows

>>> from pyspark.sql.functions import col, split  
>>> clean1= cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("tempMax", split(col("MAX"), "\\*").getItem(1))  
>>> clean2 = clean1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("tempMin", split(col("MIN"), "\\*").getItem(1))  
>>> clean3=clean2.drop('tempMax','tempMin')  
>>> clean4 = clean3.withColumn("NEW_MIN", clean3["MIN"].cast("double"))  
>>> clean5 = clean4.withColumn("NEW_MAX", clean3["MAX"].cast("double"))  
>>> clean6=clean5.drop('MAX','MIN')  
>>> clean6=clean6.withColumnRenamed("NEW_MAX","MAX")  
>>> clean6=clean6.withColumnRenamed("NEW_MIN","MIN")  
>>> rdd11=clean6.rdd
>>> clean6.createOrReplaceTempView("cleandatapushkar")
>>> 
>>> from pyspark.sql.functions import *  
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2010%') AND YEARMODA like '2010%'").show()  
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|720667|   09|  23|2010|132.8|
+------+-----+----+----+-----+

>>> output1_2010max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2010%') AND YEARMODA like '2010%'")
>>> output1_2010max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2010.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2010%') AND YEARMODA like '2010%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   08|  02|2010|-115.2|
+------+-----+----+----+------+

>>> output1_2010min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2010%') AND YEARMODA like '2010%'")
>>> output1_2010min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2010.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2011%') AND YEARMODA like '2011%'").show()   
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|720293|   06|  13|2011|131.0|
+------+-----+----+----+-----+

>>> output1_2011max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2011%') AND YEARMODA like '2011%'")
>>> output1_2011max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2011.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2011%') AND YEARMODA like '2011%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|897340|   09|  17|2011|-111.8|
+------+-----+----+----+------+

>>> output1_2011min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2011%') AND YEARMODA like '2011%'")
>>> output1_2011min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2011.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2012%') AND YEARMODA like '2012%'").show()   
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|722577|   07|  12|2012|132.8|
+------+-----+----+----+-----+

    
>>> output1_2012max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2012%') AND YEARMODA like '2012%'")
>>> output1_2012max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2012.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2012%') AND YEARMODA like '2012%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   09|  16|2012|-119.6|
+------+-----+----+----+------+

>>> output1_2012min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2012%') AND YEARMODA like '2012%'")
>>> output1_2012min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2012.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2013%') AND YEARMODA like '2013%'").show()                    
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|406890|   07|  12|2013|132.8|
+------+-----+----+----+-----+
 
>>> output1_2013max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2013%') AND YEARMODA like '2013%'")
>>> output1_2013max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2013.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2013%') AND YEARMODA like '2013%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|895770|   07|  30|2013|-115.1|
|895770|   07|  31|2013|-115.1|
+------+-----+----+----+------+

 
>>> output1_2013min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2013%') AND YEARMODA like '2013%'")
>>> output1_2013min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2013.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2014%') AND YEARMODA like '2014%'").show()          
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|406650|   08|  03|2014|129.6|
+------+-----+----+----+-----+

>>> output1_2014max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2014%') AND YEARMODA like '2014%'")
>>> output1_2014max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2014.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2014%') AND YEARMODA like '2014%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   08|  21|2014|-113.4|
+------+-----+----+----+------+

>>> output1_2014min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2014%') AND YEARMODA like '2014%'")
>>> output1_2014min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2014.csv", header='true')

>>> file_path = "/data/weather/2015"  
>>> inTextData6 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2016"                                            
>>> inTextData7 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
19/11/29 19:30:54 WARN SharedInMemoryCache: Evicting cached table partition metadata from memory due to size constraints (spark.sql.hive.filesourcePartitionFileCacheSize = 262144000 bytes). This may impact query planning performance.  
>>> inTextData7 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
>>> file_path = "/data/weather/2017"                                            
>>> inTextData8 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
>>> file_path = "/data/weather/2018"                                            
>>> inTextData9 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
>>> file_path = "/data/weather/2019"                                            
>>> inTextData10 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
>>> data20152019=inTextData6.unionAll(inTextData7).unionAll(inTextData8).unionAll(inTextData9).unionAll(inTextData10)  
>>> rdd1 = data20152019.rdd  
>>> rdd2 = rdd1.map(lambda x: str(x).split('=')[1])  
>>> rdd3 = rdd2.map(lambda x: ' '.join(x.split()))  
>>> rdd4 = rdd3.map(lambda x: x[2:-2])  
>>> rdd4.saveAsTextFile('home/amboleps/spark20152019'+'temp')  
>>> newInData = spark.read.csv('home/amboleps/spark20152019'+'temp',header=False,sep=' ')  
>>> cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')    
>>> cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
...                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
...                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
...                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
...                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
...                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
...                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
...                     .withColumnRenamed('_c21','FRSHTT')  
>>> from pyspark.sql.functions import col, split
>>> clean1= cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("tempMax", split(col("MAX"), "\\*").getItem(1))
>>> clean2 = clean1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("tempMin", split(col("MIN"), "\\*").getItem(1))
>>> clean3=clean2.drop('tempMax','tempMin')
>>> clean4 = clean3.withColumn("NEW_MIN", clean3["MIN"].cast("double"))  
>>> clean5 = clean4.withColumn("NEW_MAX", clean3["MAX"].cast("double"))  
>>> clean6=clean5.drop('MAX','MIN')  
>>> clean6=clean6.withColumnRenamed("NEW_MAX","MAX")  
>>> clean6=clean6.withColumnRenamed("NEW_MIN","MIN")  
>>> clean6.createOrReplaceTempView("cleandatapushkar")
>>> 
>>> from pyspark.sql.functions import * 
>>>
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2015%') AND YEARMODA like '2015%'").show()  
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|916700|   10|  21|2015|132.4|
+------+-----+----+----+-----+

>>> output1_2015max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2015%') AND YEARMODA like '2015%'")
>>> output1_2015max.coalesce(1).write.format('csv').save("home/amboleps/output/max2015.csv", header='true')

>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2015%') AND YEARMODA like '2015%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|895770|   09|  17|2015|-114.2|
|896060|   08|  22|2015|-114.2|
+------+-----+----+----+------+

>>> output1_2015min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2015%') AND YEARMODA like '2015%'")
>>> output1_2015min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2015.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2016%') AND YEARMODA like '2016%'").show()
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|700638|   06|  22|2016|129.0|
+------+-----+----+----+-----+

>>> output1_2016max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2016%') AND YEARMODA like '2016%'")
>>> output1_2016max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2016.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2016%') AND YEARMODA like '2016%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   07|  12|2016|-115.1|
+------+-----+----+----+------+

>>> output1_2016min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2016%') AND YEARMODA like '2016%'")
>>> output1_2016min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2016.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2017%') AND YEARMODA like '2017%'").show()  
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|917430|   04|  10|2017|129.6|
+------+-----+----+----+-----+

>>> output1_2017max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2017%') AND YEARMODA like '2017%'")
>>> output1_2017max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2017.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2017%') AND YEARMODA like '2017%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896250|   06|  20|2017|-116.0|
+------+-----+----+----+------+

>>> output1_2017min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2017%') AND YEARMODA like '2017%'")
>>> output1_2017min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2017.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2018%') AND YEARMODA like '2018%'").show()   
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|408110|   07|  02|2018|126.3|
+------+-----+----+----+-----+

>>> output1_2018max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2018%') AND YEARMODA like '2018%'")
>>> output1_2018max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2018.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2018%') AND YEARMODA like '2018%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   08|  28|2018|-116.3|
+------+-----+----+----+------+

>>> output1_2018min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2018%') AND YEARMODA like '2018%'")
>>> output1_2018min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2018.csv", header='true')

>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2019%') AND YEARMODA like '2019%'").show()  
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|956660|   01|  24|2019|121.1|
+------+-----+----+----+-----+

>>> output1_2019max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9 AND YEARMODA like '2019%') AND YEARMODA like '2019%'")
>>> output1_2019max.coalesce(1).write.format('csv').save("home/amboleps/output/Max2019.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2019%') AND YEARMODA like '2019%'").show()
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   04|  05|2019|-102.1|
+------+-----+----+----+------+

>>> output1_2019min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9 AND YEARMODA like '2019%') AND YEARMODA like '2019%'")
>>> output1_2019min.coalesce(1).write.format('csv').save("home/amboleps/output/Min2019.csv", header='true')


### II. Finding the hottest and coldest day across all years (2010 - 2019) along with station code and date

>>> file_path = "/data/weather/2010"  
>>> inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2011"                                            
>>> inTextData2 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
19/11/29 16:00:50 WARN SharedInMemoryCache: Evicting cached table partition metadata from memory due to size constraints (spark.sql.hive.filesourcePartitionFileCacheSize = 262144000 bytes). This may impact query planning performance.  
>>> inTextData2 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2012"                                           
>>> inTextData3 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2013"                                        
>>> inTextData4 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2014"                                           
>>> inTextData5 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2015"  
>>> file_path = "/data/weather/2016"                                            
>>> inTextData7 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
19/11/29 19:30:54 WARN SharedInMemoryCache: Evicting cached table partition metadata from memory due to size constraints (spark.sql.hive.filesourcePartitionFileCacheSize = 262144000 bytes). This may impact query planning performance.  
>>> inTextData7 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2017"                                            
>>> inTextData8 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2018"                                            
>>> inTextData9 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> file_path = "/data/weather/2019"                                            
>>> inTextData10 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
>>>data20102019=inTextData.unionAll(inTextData2).unionAll(inTextData3).unionAll(inTextData4).unionAll(inTextData5).unionAll(inTextData6).unionAll(inTextData7).unionAll(inTextData8).unionAll(inTextData9).unionAll(inTextData10)                                                                      
>>> rdd1 = data20102019.rdd  
>>> rdd2 = rdd1.map(lambda x: str(x).split('=')[1])  
>>> rdd3 = rdd2.map(lambda x: ' '.join(x.split()))  
>>> rdd4 = rdd3.map(lambda x: x[2:-2])  
>>> rdd4.saveAsTextFile('home/amboleps/spark20102019'+'temp')  
>>> newInData = spark.read.csv('home/amboleps/spark20102019'+'temp',header=False,sep=' ')  
>>> cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')  
>>> cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
...                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
...                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
...                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
...                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
...                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
...                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
...                     .withColumnRenamed('_c21','FRSHTT')
>>> cleanData.show(2, False)  
+------+--------+----+------+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
|STN   |YEARMODA|TEMP|DEWP  |SLP   |STP   |VISIB|WDSP|MXSPD|GUST |MAX  |MIN  |PRCP |SNDP |FRSHTT|
+------+--------+----+------+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
|996400|20100101|57.2|9999.9|1024.7|9999.9|999.9|3.8 |7.8  |999.9|58.5*|56.1*|0.00I|999.9|000000|
|996400|20100102|60.0|9999.9|1019.0|9999.9|999.9|5.0 |13.6 |999.9|63.0*|57.7*|0.00I|999.9|000000|
+------+--------+----+------+------+------+-----+----+-----+-----+-----+-----+-----+-----+------+
only showing top 2 rows

>>> from pyspark.sql.functions import col, split  
>>> clean1= cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("tempMax", split(col("MAX"), "\\*").getItem(1))  
>>> clean2 = clean1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("tempMin", split(col("MIN"), "\\*").getItem(1))  
>>> clean3=clean2.drop('tempMax','tempMin')  
>>> clean4 = clean3.withColumn("NEW_MIN", clean3["MIN"].cast("double"))  
>>> clean5 = clean4.withColumn("NEW_MAX", clean3["MAX"].cast("double"))  
>>> clean6=clean5.drop('MAX','MIN')  
>>> clean6=clean6.withColumnRenamed("NEW_MAX","MAX")  
>>> clean6=clean6.withColumnRenamed("NEW_MIN","MIN")  
>>> rdd11=clean6.rdd
>>> clean6.createOrReplaceTempView("cleandatapushkar")   
>>> 
>>> from pyspark.sql.functions import *  

>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9)").show()  
+------+-----+----+----+-----+                                                  
|   STN|MONTH|DATE|YEAR|  MAX|
+------+-----+----+----+-----+
|720667|   09|  23|2010|132.8|
|722577|   07|  12|2012|132.8|
|406890|   07|  12|2013|132.8|
+------+-----+----+----+-----+

>>> output2_20102019max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MAX from cleandatapushkar where MAX = (select MAX(MAX) from cleandatapushkar where MAX <> 9999.9)")
>>> output2_20102019max.coalesce(1).write.format('csv').save("home/amboleps/output/MaxAll.csv", header='true')
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9)").show()  
+------+-----+----+----+------+                                                 
|   STN|MONTH|DATE|YEAR|   MIN|
+------+-----+----+----+------+
|896060|   09|  16|2012|-119.6|
+------+-----+----+----+------+

>>> output2_20102019min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, MIN from cleandatapushkar where MIN = (select MIN(MIN) from cleandatapushkar where MIN <> 9999.9)")
>>> output2_20102019min.coalesce(1).write.format('csv').save("home/amboleps/output/MinAll.csv", header='true')



### III. Maximum and minimum precipitation with station code and date for year 2015
>>> file_path = "/data/weather/2015"  
>>> inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> name_list = inTextData.schema.names                                        
>>> name_list = str(name_list).strip("['']").split(' ')  
>>> rdd2 = rdd1.map(lambda x: str(x).split('=')[1])  
>>> rdd3 = rdd2.map(lambda x: ' '.join(x.split()))  
>>> rdd4 = rdd3.map(lambda x: x[2:-2])  
>>> rdd4.saveAsTextFile('home/amboleps/spark2015y'+'temp')  
>>> newInData = spark.read.csv('home/amboleps/spark2015y'+'temp',header=False,sep=' ')  
>>> cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')    
>>> cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
...                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
...                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
...                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
...                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
...                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
...                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
...                     .withColumnRenamed('_c21','FRSHTT')
>>> from pyspark.sql.functions import col, split
>>> clean1= cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("tempMax", split(col("MAX"), "\\*").getItem(1))  
>>> clean2 = clean1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("tempMin", split(col("MIN"), "\\*").getItem(1))  
>>> clean3=clean2.drop('tempMax','tempMin')  
>>> clean4 = clean3.withColumn("NEW_MIN", clean3["MIN"].cast("double"))  
>>> clean5 = clean4.withColumn("NEW_MAX", clean3["MAX"].cast("double"))  
>>> clean6=clean5.drop('MAX','MIN')  
>>> clean6=clean6.withColumnRenamed("NEW_MAX","MAX")  
>>> clean6=clean6.withColumnRenamed("NEW_MIN","MIN")  
>>> clean6.createOrReplaceTempView("cleandatapushkar")  
>>> clean7=clean6.withColumn("PRCP", split(col("PRCP"), "[A-I]").getItem(0)).withColumn("P1", split(col("PRCP"), "[A-I]").getItem(1))  
>>> clean7=clean7.drop('P1')  
>>> clean7.createOrReplaceTempView("cleandatapushkar")  
>>> from pyspark.sql.functions import *  
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, PRCP from cleandatapushkar where PRCP = (select MAX(PRCP) from cleandatapushkar where PRCP <> 99.99)").show()  
+------+-----+----+----+----+                                                   
|   STN|MONTH|DATE|YEAR|PRCP|
+------+-----+----+----+----+
|419890|   07|  24|2015|9.92|
|645510|   11|  23|2015|9.92|
|983340|   12|  16|2015|9.92|
|418621|   08|  21|2015|9.92|
+------+-----+----+----+----+

>>> output3max=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, PRCP from cleandatapushkar where PRCP = (select MAX(PRCP) from cleandatapushkar where PRCP  99.99)")
>>> output3max.coalesce(1).write.format('csv').save("home/amboleps/output/MaxPrecipitation2015.csv", header='true')     
>>> spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, PRCP from cleandatapushkar where PRCP = (select MIN(PRCP) from cleandatapushkar where PRCP <> 99.99)").show()
Hive Session ID = cc95321d-f1f2-455c-af8e-3d78d896315f     
+------+-----+----+----+----+                                                   
|   STN|MONTH|DATE|YEAR|PRCP|
+------+-----+----+----+----+
|727530|   07|  03|2015|0.00|
|727535|   05|  24|2015|0.00|
|727584|   03|  18|2015|0.00|
|727584|   09|  12|2015|0.00|
|727584|   11|  21|2015|0.00|
|727630|   09|  27|2015|0.00|
|727640|   06|  08|2015|0.00|
|727670|   01|  17|2015|0.00|
|727676|   05|  31|2015|0.00|
|727676|   08|  11|2015|0.00|
|727677|   08|  04|2015|0.00|
|727684|   10|  11|2015|0.00|
|727684|   11|  09|2015|0.00|
|727686|   01|  27|2015|0.00|
|727686|   02|  27|2015|0.00|
|727686|   07|  03|2015|0.00|
|727686|   07|  24|2015|0.00|
|727686|   11|  18|2015|0.00|
|727690|   01|  22|2015|0.00|
|727720|   06|  23|2015|0.00|
+------+-----+----+----+----+
only showing top 20 rows

>>> c7=clean7.where("PRCP='0.00'")  
>>> c7.count()  
2427220                                                                         

>>> output3min=spark.sql("Select distinct(STN), SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, PRCP from cleandatapushkar where PRCP = (select MIN(PRCP) from cleandatapushkar where PRCP <> 99.99)")
>>> output3min.coalesce(1).write.format('csv').save("home/amboleps/output/MinPrecipitation2015.csv", header='true')
>>>                                                                             


### IV. Count of percentage missing values for mean station pressure (STP) for year 2019 and stations
>>> file_path = "/data/weather/2019"  
>>> inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)  
>>> name_list = inTextData.schema.names                                         
>>> name_list = str(name_list).strip("['']").split(' ')  
>>> names = []  
>>> for item in name_list:  
...     if len(item)>0:  
...         names.append(item)  
...     
>>> rdd1 = inTextData.rdd
>>> rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
>>> rdd3 = rdd2.map(lambda x: ' '.join(x.split()))       
>>> rdd4 = rdd3.map(lambda x: x[2:-2])  
>>> rdd4.saveAsTextFile('home/amboleps/spark2019y'+'temp')  
>>> newInData = spark.read.csv('home/amboleps/spark2019y'+'temp',header=False,sep=' ')  
>>> cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')     
>>> cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
...                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
...                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
...                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
...                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
...                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
...                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
...                     .withColumnRenamed('_c21','FRSHTT') 
>>> from pyspark.sql.functions import col, split  
>>> clean1= cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("tempMax", split(col("MAX"), "\\*").getItem(1))  
>>> clean2 = clean1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("tempMin", split(col("MIN"), "\\*").getItem(1))  
>>> clean3=clean2.drop('tempMax','tempMin')   
>>> clean4 = clean3.withColumn("NEW_MIN", clean3["MIN"].cast("double"))        
>>> clean5 = clean4.withColumn("NEW_MAX", clean3["MAX"].cast("double"))  
>>> clean6=clean5.drop('MAX','MIN')      
>>> clean6=clean6.withColumnRenamed("NEW_MAX","MAX")
>>> clean6=clean6.withColumnRenamed("NEW_MIN","MIN")  
>>> clean6.createOrReplaceTempView("cleandatapushkar")    
>>> from pyspark.sql.functions import *  
>>> totalVal=(float)(clean6.count())                                                
>>> nonMissingFilter=clean6.where("STP!='9999.9'")                              
>>> nonMissingVal=(float)(nonMissingFilter.count())  
>>> percentageNonMissingVal= (nonMissingVal/totalVal) * 100 
>>> percentageMissingVal = 100 - percentageNonMissingVal  
>>> print(percentageMissingVal)  
27.961149261
>>> 


### V. Station code with maximum wind gust and date for year 2019
>>> file_path = "/data/weather/2019"  
>>> inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)    
>>> name_list = inTextData.schema.names                                         
>>> name_list = str(name_list).strip("['']").split(' ')  
>>> names = []  
>>> for item in name_list:    
...     if len(item)>0:  
...         names.append(item)  
...  
>>> rdd1 = inTextData.rdd    
>>> rdd2 = rdd1.map(lambda x: str(x).split('=')[1])     
>>> rdd3 = rdd2.map(lambda x: ' '.join(x.split()))   
>>> rdd4 = rdd3.map(lambda x: x[2:-2])
>>> rdd4.saveAsTextFile('home/amboleps/spark2019y'+'temp')  
>>> newInData = spark.read.csv('home/amboleps/spark2019y'+'temp',header=False,sep=' ')    
>>> cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')       
>>cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
...                     .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
...                     .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
...                     .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
...                     .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
...                     .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
...                     .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
...                     .withColumnRenamed('_c21','FRSHTT')

>>> from pyspark.sql.functions import col, split  
>>> clean1= cleanData.withColumn("MAX", split(col("MAX"), "\\*").getItem(0)).withColumn("tempMax", split(col("MAX"), "\\*").getItem(1))  
>>> clean2 = clean1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0)).withColumn("tempMin", split(col("MIN"), "\\*").getItem(1))  
>>> clean3=clean2.drop('tempMax','tempMin')
>>> clean4 = clean3.withColumn("NEW_MIN", clean3["MIN"].cast("double"))  
>>> clean5 = clean4.withColumn("NEW_MAX", clean3["MAX"].cast("double"))  
>>> clean6=clean5.drop('MAX','MIN')
>>> clean6=clean6.withColumnRenamed("NEW_MAX","MAX") 
>>> clean6=clean6.withColumnRenamed("NEW_MIN","MIN")  
>>> clean6.createOrReplaceTempView("cleandatapushkar")  
>>> from pyspark.sql.functions import *  
>>> spark.sql("Select STN, SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, GUST from cleandatapushkar where GUST = (select MAX(GUST) from cleandatapushkar where GUST <> 999.9)").show() 
Hive Session ID = 26d0722d-646f-444c-bbfe-fa524e75b426  
+------+-----+----+----+----+                                                   
|   STN|MONTH|DATE|YEAR|GUST|
+------+-----+----+----+----+
|726130|   02|  25|2019|99.1|
+------+-----+----+----+----+

>>> output5=spark.sql("Select STN, SUBSTRING(YEARMODA,5,2) as MONTH, SUBSTRING(YEARMODA,7,2) as DATE, SUBSTRING(YEARMODA,1,4) as YEAR, GUST from cleandatapushkar where GUST = (select MAX(GUST) from cleandatapushkar where GUST <> 999.9)")
>>> output5.coalesce(1).write.format('csv').save("home/amboleps/output/MaxGust2019.csv", header='true')
