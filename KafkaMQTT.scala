package in.skylinelabs.spark


import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import kafka.serializer.StringDecoder

object KafkaMQTT {
  
  def convertRowToJSON(row: Row): String = {
            val m = row.getValuesMap(row.schema.fieldNames)
            JSONObject(m).toString()
  }
  
  def main(args: Array[String]) {
   
    val sparkConf = new SparkConf().setAppName("Kafka").set("spark.scheduler.mode", "FAIR").setMaster("local[2]").set("spark.app.id", "Kafka")    //Variable
    val ssc = new StreamingContext(sparkConf, Seconds(1))   
    val sc = ssc.sparkContext
    val sqc = new SQLContext(sc)
    
 
/****************************************MQTTInitialize***************************************************/     
    val brokerUrl = "tcp://35.162.23.96:1883"                                                 //Variable
    var client: MqttClient = null
    val persistence = new MqttDefaultFilePersistence("/tmp")
    client = new MqttClient(brokerUrl, MqttClient.generateClientId, persistence)
    var messageMQTT = new MqttMessage()
    messageMQTT.setQos(0)
    
    try{
      client.connect()
      
    } catch {
      case e: Exception => println("No Internet :(")
    }

/****************************************MQTT***************************************************/     
    
    

    
    
    val topicsSet = Set("test")
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
        temp.createOrReplaceTempView("scheme")
        val query = "SELECT * FROM scheme WHERE temperature > 20"                                  //Variable SQL Query
        val queryResult = sqc.sql(query)
        
        val number = queryResult.count().toInt 
        val result = queryResult.collect()
        
        result.foreach(f=>{
            val mes = convertRowToJSON(f)
            
            
/****************************************MQTT***************************************************/
           
            messageMQTT = new MqttMessage(mes.toString.getBytes("utf-8"))
            messageMQTT.setQos(0)
            
            val topic = "test"
    
            if(client.isConnected()){
              var msgTopic = client.getTopic(topic)
              msgTopic.publish(messageMQTT)
            }
            else{
              try { 
                  client.connect()
                  var msgTopic = client.getTopic(topic)
                  messageMQTT = new MqttMessage(mes.toString.getBytes("utf-8"))
                  messageMQTT.setQos(0)
                  msgTopic.publish(messageMQTT)                  
               } catch {
                 case e: Exception => println("No Internet :(")
               }
            }
            
/****************************************MQTT***************************************************/       
        })       
      }
    })
        
    ssc.start()
    ssc.awaitTermination()
  }
}

