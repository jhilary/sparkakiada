import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{DStream, PairDStreamFunctions}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import scopt.OptionParser

case class Config(zkquorum: String = "5.9.102.237:2181", group: String = "weblogs",
                  topic: String = "weblogs", numPartitions: Int = 1,
                  master: String = "yarn-client", procedure: Int = 1, batchDuration: Int = 10)

case class LogRecord(ip: String, time: String, request: String, response: Int,
                     responseSize: Int, url: String, userAgent: String)

object LogRecord {
  def parse(line: String): LogRecord = {
    val Array(ip, _, _, time, request, response, responseSize, url, userAgent) = line split "\t"
    LogRecord(ip, time, request, response.toInt, responseSize.toInt, url, userAgent)
  }
}

case class A(url:String, count:Int) extends Ordered[A] {
  def compare(x: A) =  -count.compareTo(x.count)
}
object UrlOrdering extends Ordering[(String,Int)] {
  def compare(a:(String,Int), b:(String,Int)) = a._2.compare(b._2)
}

object Test {

  def isIPStartedByOne(ip: String): Boolean = (ip split "\\." head) contains "1"

  def printCountOfIpsStartedByOne(lines: DStream[String]): Unit = {
    val ipStartedByOneCount = lines.map(LogRecord.parse).map(_.ip).filter(isIPStartedByOne).countByValue()
    ipStartedByOneCount.print()
  }

  def saveAvgResponseForEachIP(lines: DStream[String], filePath: String): Unit = {
    val avgResposeSize = lines.map(LogRecord.parse).map(x => (x.ip, (x.responseSize, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(x => x._1.toFloat/x._2)
    val formatedString = avgResposeSize.map(x => "%s\t%s".format(x._1, x._2))
    formatedString.saveAsTextFiles(filePath)
  }

  def updateTop100State(newValue: Seq[Long], currentCount: Option[Int]): Option[Int] = {
    val newCount = newValue.sum
    Some(currentCount.getOrElse(0) + newCount.toInt)
  }

  def saveTop100urls(context: StreamingContext, lines: DStream[String], filePath: String): Unit = {
    lines.print()
    val state = lines.map(LogRecord.parse).map(_.url).countByValue().updateStateByKey[Int](updateTop100State _)
    val top = state.transform(x => context.sparkContext.parallelize(x.top(100)(UrlOrdering)))
    top.map(x => "%s\t%s".format(x._1, x._2)).saveAsTextFiles(filePath)
  }

  def main(args: Array[String]) {

    val parser = new OptionParser[Config]("scopt") {
      head("scopt", "3.x")

      opt[String]('z', "zkquorum") action {
        (x, c) => c.copy(zkquorum = x)
      } text "zookeper quorum ip and port in format <ip>:<port>"

      opt[String]('g', "group") action {
        (x, c) => c.copy(group = x)
      } text "kafka consumer group name"

      opt[(String, Int)]('t', "topic") action {
        case ((k, v), c) => c.copy(topic = k, numPartitions = v)
      } keyValueName("<topic>", "<numPartitions>") text "topics - numPartitions map"

      opt[String]('m', "master") action {
        (x, c) => c.copy(master = x)
      } text "spark master option"

      opt[Int]('p', "procedure") action {
        (x, c) => c.copy(procedure = x)
      } validate { x =>
        if (Set(1,2,3,4) contains x) success else failure("Procedure can take values (1,2,3) only")
      } text "dz procedure number"

      opt[Int]('b', "batchDuration") action {
        (x, c) => c.copy(batchDuration = x)
      } text "streaming context batch duration"

      note("This is attempt to use options parser.\n")
      help("help") text "prints this usage text"
    }
    parser.parse(args, Config()) map { config =>

      println(config.master)
      val sparkConf = new SparkConf().setMaster(config.master).setAppName("SparkStreamingIlariia")
      val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))
      val lines = KafkaUtils.createStream(ssc, config.zkquorum, config.group, Map(config.topic -> config.numPartitions)).map(_._2)
      ssc.checkpoint("checkpoint")
      if (config.procedure == 1) {
        printCountOfIpsStartedByOne(lines)
      } else if (config.procedure == 2) {
        val filePath = "hdfs://hadoop2-00.yandex.ru:8020/user/ilariia/dz3/2/avg-response"
        saveAvgResponseForEachIP(lines, filePath)
      } else if (config.procedure == 3) {
        val filePath = "hdfs://hadoop2-00.yandex.ru:8020/user/ilariia/dz3/3/saveTop100urls"
        saveTop100urls(ssc, lines, filePath)
      }


      ssc.start()
      ssc.awaitTermination()

    } getOrElse {

      Console.err.println("Bad options. Please take a look on help")
    }
  }
}
