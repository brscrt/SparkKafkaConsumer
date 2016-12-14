package db

import com.mongodb.MongoClient
import com.mongodb.casbah.Imports._
import Common._

object MyMongoDb {  
	
  def writeDb(table:String,key:String,value:Float){
    val mongoClient = new MongoClient("localhost", 27017);
	  val db = mongoClient.getDatabase("datas");
    val collection = db.getCollection(table);
    
    val mongoObj = buildMongoDbObject(Stock(key,value))
        MongoFactory.collection.save(mongoObj)
  
    println("result is added to db")
  }
  
  
	
	
}