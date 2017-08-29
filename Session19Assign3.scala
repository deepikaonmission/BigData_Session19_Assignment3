/* <<<<<<<<<<<---------- QUERY ------------>>>>>>>>>>>>>
1. Create a dataframe with 1 to 100 and save as parquet file
*/

import org.apache.spark.sql.{Row,Column,SparkSession,SQLContext}  //Explanation is already given in Assignment18.1
//System.setProperty("hadoop.home.dir","F:/Softwares/winutils")    //error: expected class or object definition
object Session19Assign3 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session19Assign3")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment3")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1
  //config is used to provide path to the warehouse directory, if config is not provided, then error is returned

  //setting path of winutils.exe
  System.setProperty("hadoop.home.dir","F:/Softwares/winutils")
  //winutils.exe needs to be present inside HADOOP_HOME directory to read and write parquet file, else below error is returned:
  //error: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

  val rdd1 = spark.sparkContext.parallelize(1 to 100)    //created rdd1 containing range 1 to 100
  //rdd1 -->> RDD[Int]

  rdd1.foreach(x => println(x))
  //REFER Screenshot 1 for OUTPUT

  //Creation of DataFrame from RDD
  import spark.implicits._       //import is required, because toDF is used to convert RDD to DataFrame

  //Converting rdd to dataframe
  val df1 = rdd1.toDF("rangeFrom1to100")     //df1 -->> sql.DataFrame

  df1.printSchema()
  //REFER Screenshot 2 for OUTPUT

  df1.show()
  //REFER Screenshot 3 for OUTPUT

  //saving parquet file at given location
  df1.select("rangeFrom1to100").write.format("parquet").save("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment3/range.parquet")
  //REFER Screenshots 4,5,6
  //Screenshot 4 ->  shows catalyst schema for parquet data
  //Screenshot 5 -> shows range.parquet folder is created inside G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment3/
  //Screenshot 6 -> shows 4 files i.e. _SUCCESS.crc, part.crc, _SUCCESS and part
  //if above folder and files are created, it means parquet file has been saved successfully

  //verifying whether data is written successfully inside parquet file or not
  //reading parquet file from given location [only if above "save" query works fine]
  val df2 = spark.sqlContext.read.parquet("G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment3/range.parquet")

  df2.printSchema()                              //df2 -->> sql.DataFrame
  //REFER Screenshot 7 for OUTPUT

  df2.show()
  //REFER Screenshot 8 for OUTPUT

}




