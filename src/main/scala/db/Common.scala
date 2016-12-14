package db

import com.mongodb.casbah.Imports._

case class Stock (topic: String, result: Float)

object Common {
  /**
   * Convert a Stock object into a BSON format that MongoDb can store.
   */
  def buildMongoDbObject(stock: Stock): MongoDBObject = {
      val builder = MongoDBObject.newBuilder
      builder += "topic" -> stock.topic
      builder += "result" -> stock.result
      builder.result
  }
}