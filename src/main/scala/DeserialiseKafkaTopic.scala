import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.Base64
import metricMessage.MetricOuterClass.Metric
import org.apache.spark.sql.Encoders

object DeserialiseKafkaTopic extends App {

val spark = SparkSession.builder()
    .appName("Serialise-Kafka-Topic")
    .master("local[2]") 
    .config("spark.ui.port", "4060")
    .getOrCreate()

  import spark.implicits._

  // Read from Kafka
  val kafkaDataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "demetric")
    .option("startingOffsets", "earliest")
    .load()

  val deserializedData = kafkaDataFrame
  .selectExpr("CAST(value AS STRING) as encoded_value")
  .as[String]
  .flatMap { encodedValue =>
    val bytes = Base64.getDecoder.decode(encodedValue)
    val metric = Metric.parseFrom(bytes)
    try {
      Some((
        metric.getHost,
        metric.getMetricName,
        metric.getRegion,
        metric.getTimestamp,
        metric.getValue
      ))
    } catch {
      case e: Exception => None
    }
  }(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.scalaInt))
  .toDF("host", "metricName", "region", "timestamp", "value")

  
  val query = deserializedData
    .writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "/Users/ronak/Downloads/csv_files12")
    .option("checkpointLocation", "/Users/ronak/Desktop/csv_checkpoints123")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .partitionBy("metricName")
    .start()

  query.awaitTermination()
}