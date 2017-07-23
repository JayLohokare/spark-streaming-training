package in.skylinelabs.spark

import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import javax.mail._
import javax.mail.internet._
import java.util.Properties._


import kafka.serializer.StringDecoder

object KafkaEmail {
  
  def convertRowToJSON(row: Row): String = {
            val m = row.getValuesMap(row.schema.fieldNames)
            JSONObject(m).toString()
  }
  
  def main(args: Array[String]) {
   
    var inputTopic = "test"
    
    val sparkConf = new SparkConf().set("spark.scheduler.mode", "FAIR").setAppName("Kafka").setMaster("local[2]").set("spark.app.id", "Kafka")    //Variable
    val ssc = new StreamingContext(sparkConf, Seconds(1))   
    val sc = ssc.sparkContext
    val sqc = new SQLContext(sc)
    val topicsSet = Set(inputTopic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092")              //Variable
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    lines.print()
    var message = lines.toString()
    
    lines.foreachRDD(rdd=>{

      if(rdd.isEmpty()){}  
      else{
        
        val temp = sqc.read.json(rdd.filter(JSON.parseFull(_).isDefined))
        temp.printSchema()
        temp.createOrReplaceTempView(inputTopic)
        val query = "SELECT * FROM " + inputTopic + " WHERE temperature > 20"                                  //Variable SQL Query
        val queryResult = sqc.sql(query)
        
        val number = queryResult.count().toInt 
        val result = queryResult.collect()
        
        result.foreach(f=>{
            val mes = convertRowToJSON(f)

/***************************************Email***************************************/   
            var bodyText = mes
            val properties = System.getProperties
            properties.put("mail.smtp.host", "localhost")
            val session = Session.getDefaultInstance(properties)
            val message = new MimeMessage(session)
            message.setFrom(new InternetAddress("noreply@sparkstreamingemail.in"))
            message.setRecipients(Message.RecipientType.TO, "reshul.dani@gmail.com")
            message.setSubject("Greetings from Jay using Spark Streaming Email service")
            message.setText(bodyText)
            Transport.send(message)
            
/***************************************Email***************************************/   
        })       
      }
    })
        
    ssc.start()
    ssc.awaitTermination()
  }
}

