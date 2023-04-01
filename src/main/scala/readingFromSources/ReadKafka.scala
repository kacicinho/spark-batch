package readingFromSources

import org.apache.spark.sql.SparkSession

object ReadKafka extends App {

  // build a session in spark
  val sparkSession = SparkSession.builder()
    .appName("kafkaReader")
    .config("spark.master", "local")
    .getOrCreate()

  // read from kafka for batch

  val kafkaDf = sparkSession.read
    .format("kafka")
    .option("kafka.bootstrap.server", "host1:port1, host2:port2")
    .option("subscribe, topic1")
    .load()

  // Subscribe to a pattern, at the earliest and latest offsets
  /*
  it's start to read from the earliest item and end at the latest item
   */
  val df = sparkSession.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribePattern", "topic.*")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()

}
