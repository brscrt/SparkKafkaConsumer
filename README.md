# SparkKafkaConsumer

This project consumes data from Apache Kafka Producer service and calculate volume rated average price for every topic via Apache Spark.

This is kafka producer project that produce data for this consumer project: https://github.com/brscrt/KafkaProducer

## Adding dependencies

To use libraries .sbt is used. This dependecies are needed:
```sh
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
libraryDependencies += "org.mongodb" %% "casbah" % "2.5.0"
```
## Generating project files for eclipse

go to the path of the folder that has build.sbt file and write on terminal `sbt eclipse`. This gives eclipse project files. Then in eclipse, import this project.

## Consumer.scala
This class listens the given topics, calculates volume rated price value and writes the result on mongodb
```scala
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
         if (payda != 0){
            val res:Float=(pay / payda)
            println("topic : " + k + " result = " + res)
            MyMongoDb.writeDb("spark", k, res)
          }
        }
      }

    })

  }
}
```
## Dockerize the project

To dockerize, javaapp and docker plugins must be added to build.sbt like below:
```sh
enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging)
```
Then the tasks need to be writen to generate dockerfile and image. Example tasks:

```sh
dockerfile in docker := {
  val appDir: File = stage.value
  val targetDir = "/app"

  new Dockerfile {
    from("java")
    entryPoint(s"$targetDir/bin/${executableScriptName.value}")
    copy(appDir, targetDir)
  }
}

imageNames in docker := Seq(
  // Sets the latest tag
  ImageName(s"brscrt/sparkkafkaconsumer")

)
```
Finally, come again build.sbt path and write `sbt docker`. This gives dockerfile and docker image. 
