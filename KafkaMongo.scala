package in.skylinelabs.spark

import scala.util.parsing.json.JSONObject

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.bson.Document

import com.mongodb.spark.MongoSpark

object Mongo {
  
 def convertRowToJSON(row: Row): String = {
            val m = row.getValuesMap(row.schema.fieldNames)
            JSONObject(m).toString()
  }
 def main(args: Array[String]){
   

    val uri: String = "mongodb://localhost/rules.rules"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MongoSparkConnectorTour")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val ssc = new StreamingContext(conf, Seconds(1))  
    val sc = ssc.sparkContext   
    val sqc = new SQLContext(sc)
    
    val firstRDD = MongoSpark.load(sc)
    val rddMongo = firstRDD.filter(doc => doc.getString("kafka_topic")=="jay")
    
                println(rddMongo)
                if(rddMongo.isEmpty()){
                  println("Shhhhhiiiitttttt")
                }
        
                else{
                  rddMongo.foreach(f=>{
                    val action = f.getString("action")
                    val query = f.getString("query")
                    
                    println(query)
                    println(action)
                  })
                }
 
    
    
    /*
     * 
     * 
     *
    

    val topicsSet = Set("test")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
           
      
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    lines.print()
    var message = lines.toString()
    
    lines.foreachRDD(rdd=>{

      if(rdd.isEmpty()){
        println("Empty :(")
      }  
      else{
        
        val temp = sqc.read.json(rdd.filter(JSON.parseFull(_).isDefined))
        temp.printSchema()
        temp.createOrReplaceTempView("scheme")
        val query = "SELECT * FROM scheme WHERE temperature > 20"                          //Variable
        val queryResult = sqc.sql(query)
        
        val number = queryResult.count().toInt 
        val result = queryResult.collect()
        MongoSpark.save(queryResult.write.mode("append"))

      }
    })
    * 
    * 
    */
    
            
    ssc.start()
    ssc.awaitTermination()          
  }
}