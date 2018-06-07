package streaming
import Logs.LogHHB
import kafka.serializer.StringDecoder
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import utils.SparkUtils.getSQLContext
import org.apache.spark.sql.types._

import scala.util.Try
import scala.util.parsing.json._
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import config.Settings

import scala.collection.mutable
import scala.util._
import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global



object streaming_j_1 extends  App{


    def rddToRDDActivity_2(input: RDD[(String,String)]) = {
      input.mapPartitionsWithIndex {(index, it) =>
        it.flatMap {kv=>
          val result = JSON.parseFull(kv._2)
          result match {
            //case Some(m: Map[String, _]) => Some(Logs.LogHHB((m("guid"),m("Location"),m("eventTime"),m("consumed"),m("generated")))) // or m.get("name") for an Option
            //case Some(m: Map[String, _]) => Some(Map("guid"-> m("guid"),"Location"-> m("Location"),"eventTime"-> m("eventTime"),"consumed"-> m("consumed"),"generated"-> m("generated"))) // or m.get("name") for an Option
            case Some(m: Map[String, _])=> Some(LogHHB(m("guid").toString,m("Location").toString, (parse_apache_time(m("eventTime").toString)).toString,(parse_apache_time_h(m("eventTime").toString)).toString,m("consumed").toString,m("generated").toString)) // or m.get("name") for an Option
            case _ => sys.error("Failed.")
          }
        }
      }
    }
    def parse_apache_time(apache_time: String) : String = {
      val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(apache_time)
      import java.text.DateFormat
      import java.text.SimpleDateFormat
      //val df = new SimpleDateFormat("yyyy-MM-dd:HH")
      val df = new SimpleDateFormat("YYYY-MM-dd HH:00")
      val date_e = df.format(date_c)//+ "+0000"
      //      val cal = Calendar.getInstance()
      //     cal.setTime(date)
      //     cal
      date_e
    }
    def parse_apache_time_h(apache_time: String) : String = {
      val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(apache_time)
      import java.text.DateFormat
      import java.text.SimpleDateFormat
      val df = new SimpleDateFormat("HH")
      val date_e = df.format(date_c)
      //      val cal = Calendar.getInstance()
      //     cal.setTime(date)
      //     cal
      date_e
    }
    val HHL = Settings.HHLogGen
    val conf = new SparkConf()
      .setAppName("pruebas")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir("/home/bju/data_app/data_EM/CPs/")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._
    val batch_duration = Seconds(30)
    val ssc = new StreamingContext(sc, batch_duration)
    val kafkaDirectParams = Map(
      "metadata.broker.list" -> "localhost:9092",
      "group.id" -> "lambda",
      "auto.offset.reset" -> "largest"
    )
    val topic = "inst"
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectParams, Set(topic)
    )
    val d = kafkaDirectStream.transform(input => {
      rddToRDDActivity_2(input)
    }).cache()
    val df_3= d.foreachRDD(  rdd =>{
      val activityDF = rdd.toDF()
      //activityDF.write.mode(SaveMode.Append).partitionBy("inst").parquet(HHL.hdfsPath)
      activityDF.write.mode(SaveMode.Append).parquet("/home/bju/data_app/data_EM/data_2/")
     })
    val stateful = d.transform( rdd => {
      val df = rdd.toDF()
      val df2 = df.selectExpr(
        "inst",
        "eventTime",
        "eventHour",
        "cast(consumed as long) consumed",
        "cast(generated as long) generated"
      )
      val activityByHH = df2.groupBy("inst","eventTime","eventHour").agg(sum("generated"),sum("consumed"),count("consumed"))
      activityByHH.map{ r => ((r.getString(0), r.getString(1), r.getString(2)),Logs.HH_Agg_Akk(r.getLong(3),r.getLong(4),r.getLong(5)))}
    }).updateStateByKey((newItemsPerKey:Seq[Logs.HH_Agg_Akk],currentState:Option[(Long,Long,Long,Long)])=>{
      var(prevT, gen, con,stat) = (currentState.getOrElse(System.currentTimeMillis(),0L,0L,0L))
      var result : Option[(Long,Long,Long,Long)] = null
      if (newItemsPerKey.isEmpty){
        if(System.currentTimeMillis()-prevT> 1000*60*60)
          result=None
        else
          result = Some((prevT, gen, con,stat))
      }else{
        newItemsPerKey.foreach(a=>{
          gen = a.generated+gen
          con = a.consumed+con
          stat = a.counter+stat
        })
        result = Some((System.currentTimeMillis(), gen,con,stat))
      }
      result
    })


    stateful
       .reduceByKeyAndWindow(
        (a, b) => b,
        (x, y) => x,
        Seconds(60)
        )
      .map( sr =>Logs.HH_Agg_DB( sr._1._1, sr._1._2, sr._1._3, (sr._2._2).toString,  (sr._2._3).toString))
       .saveToCassandra("em", "stream_hh_c")

//No Funciona Future
  stateful
    .reduceByKeyAndWindow(
      (a, b) => b,
      (x, y) => x,
      Seconds(60)
    ).map( sr =>Logs.HH_Agg_DB_Akk( sr._1._1, sr._1._2, sr._1._3, (sr._2._2).toString,  (sr._2._3).toString,(sr._2._4).toString))
    .filter(a=>a.counter=="4")
    .foreachRDD(b=>{
      b.map(c=>Future(batch.gen_model.start(c.inst)))
    })


  ssc.start()
  ssc.awaitTermination()




}
