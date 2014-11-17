import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import scopt.OptionParser

case class Config(zkquorum: String = "5.9.102.237:2181", group: String = "weblogs",
                  topic: String = "weblogs", numPartitions: Int = 1,
                  master: String = "yarn-client")

object Test{

  def main(args: Array[String]) {

    val parser = new OptionParser[Config]("scopt") {
      head("scopt", "3.x")

      opt[String]('z', "zkquorum") action {
        (x, c) => c.copy(zkquorum = x)
      } text "zookeper quorum ip and port in format <ip>:<port>"

      opt[String]('g', "group")  action {
        (x, c) => c.copy(group = x)
      } text "kafka consumer group name"

      opt[(String, Int)]('t', "topic") action {
        case ((k, v), c) => c.copy(topic = k, numPartitions = v)
      } keyValueName("<topic>", "<numPartitions>") text "maximum count for <libname>"

      opt[String]('m', "master") action {
        (x, c) => c.copy(master = x)
      } text "spark master option"

      note("This is attempt to use options parser.\n")
      help("help") text "prints this usage text"
    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      println(config.master)

      val sparkConf = new SparkConf().setMaster(config.master).setAppName("SparkStreamingIlariia")
      val ssc =  new StreamingContext(sparkConf, Seconds(2))
      ssc.checkpoint("checkpoint")

      val lines = KafkaUtils.createStream(ssc, config.zkquorum, config.group, Map(config.topic -> config.numPartitions)).map(_._2)
      lines.count().print()
      ssc.start()
      ssc.awaitTermination()
    } getOrElse {
      Console.err.println("Bad options. Please take a look on help")
    }

//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
//
//    val Array(zkQuorum, group, topics, numThreads) = args
//    val sparkConf = new SparkConf().setMaster("yarn-client").setAppName("SparkStreamingIlariia")
//    val ssc =  new StreamingContext(sparkConf, Seconds(2))
//    ssc.checkpoint("checkpoint")
//
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, Map(("weblogs"->1))).map(_._2)
//
//
//    ssc.start()
//    ssc.awaitTermination()
  }
}