package kafka

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream.InputDStream

object Consumer {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Linear Alignment").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(8))

    val brokers = "172.17.0.3:9092";

    val topicsSet = Set("TEKTU", "GUBRF", "mytopic")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    calculate(messages)

    ssc.start();
    ssc.awaitTermination();
  }

  def calculate(messages: InputDStream[(String, String)]) {
    val price = 7
    val volume = 9

    messages.foreachRDD(rdd => {
      var pay: Float = 0
      var payda: Float = 0
      var topic = ""

      val map = rdd.mapPartitions(datas => {
        datas.map(data => {
          (data._2.split(",")(1), data._2)
        })
      })
      map.groupByKey().foreach {
        case (k, v) => {
          v.foreach(i => {
            val sequence = i.split(",")
            pay += sequence(price).toFloat * sequence(volume).toFloat
            payda += sequence(volume).toFloat
          })
          if (payda != 0)
            println("topic : " + k + " result = " + pay / payda)
        }
      }

    })

  }
}