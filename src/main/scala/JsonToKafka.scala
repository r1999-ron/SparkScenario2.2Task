import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.sql.streaming.Trigger
import java.util.Base64
import metricMessage.MetricOuterClass.Metric

object JsonToKafkaTopic extends App{
  val spark = SparkSession.builder()
    .appName("KafkaProtobufJob")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val schema = new StructType()
    .add("host", StringType)
    .add("metricName", StringType)
    .add("region", StringType)
    .add("timestamp", StringType)
    .add("value", IntegerType)

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "metric-message")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) as json")

  val metrics = df.select(from_json($"json", schema).as("data")).select("data.*")

  
  val protobufData = metrics.map { row =>
    import metricMessage.MetricOuterClass.Metric

    val metric = Metric.newBuilder()
      .setHost(row.getAs[String]("host"))
      .setMetricName(row.getAs[String]("metricName"))
      .setRegion(row.getAs[String]("region"))
      .setTimestamp(row.getAs[String]("timestamp"))
      .setValue(row.getAs[Int]("value"))
      .build()
    Base64.getEncoder.encodeToString(metric.toByteArray)
  }

  

  val query = protobufData
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "demetric")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "/Users/ronak/Desktop/kafka_spark102")
    .start()



  query.awaitTermination()
}