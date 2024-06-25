import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object ProcessCsv extends App {
  // Initialize Spark Session
  val spark = SparkSession.builder()
    .appName("CSV Folder Processor")
    .master("local[*]") 
    .config("spark.ui.port", "4080")
    .getOrCreate()

  import spark.implicits._

  // Function to get list of subdirectories within a directory
  def getListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName)
      .listFiles
      .filter(_.isDirectory)
      .filter(dir => !dir.getName.contains("_spark_metadata"))
      .map(_.getPath)
  }

  // Function to perform aggregation on DataFrame
  def performAggregation(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val aggDF = df.groupBy($"_c0".alias("server_name"))  
      .agg(
        sum("_c3").alias("total_sum"),    
        count("*").alias("count"),        
        avg("_c3").alias("average")       
      )
    aggDF
  }

  // Function to process each folder
  def processFolder(folderPath: String): Unit = {
    println(s"Processing folder: $folderPath")

    val df = spark.read
      .option("header", "false") 
      .option("inferSchema", "true")
      .csv(s"$folderPath/*.csv")

    val aggregatedDF = performAggregation(df)

    val recordCount = df.count()

    val writer = new PrintWriter(new File(s"$folderPath/count.txt"))
    try {
      writer.write(s"Total records: $recordCount\n")
    } finally {
      writer.close()
    }

    // Show aggregated results for each server_name
    println(s"Aggregated results for folder: $folderPath")
    aggregatedDF.show()
  }

  // Main entry point
  val baseDirectory = "/Users/ronak/Downloads/csv_files12"

  // Get list of subdirectories within the base directory
  val subFolders = getListOfSubDirectories(baseDirectory)

  // Process each subfolder
  subFolders.foreach(processFolder)

  // Stop the Spark session
  spark.stop()
}
