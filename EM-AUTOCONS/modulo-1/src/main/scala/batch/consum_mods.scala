package batch

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import utils.SparkUtils
import utils.SparkUtils.{getSQLContext, getSparkContext}
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.driver.core.Row
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

object consum_mods {

  def parse_apache_time_wd(apache_time: String) = {

    val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(apache_time)


    //val df = new SimpleDateFormat("yyyy-MM-dd:HH")
    val df = new SimpleDateFormat("EEE")
    val date_e = df.format(date_c)
    date_e
  }

  def train_cons() = {
    val conf = new SparkConf()
      .setAppName("pruebas")
      .setMaster("local[*]")
      .set("spark.casandra.connection.host", "localhost")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = getSQLContext(sc)
    val hc = new HiveContext(sc)


    //val tmp_4 = hc.read.parquet("/home/bju/data_app/data_EM/data_2/")
    val tmp_4 = hc.read.parquet(config.Settings.HHLogGen.hdfsPath).cache()
    import org.apache.spark.sql.functions._

    val tmp_4_b= trns(tmp_4).cache()
    tmp_4_b.show(50)
    val tmp_5 = tmp_4_b.select("inst").distinct.collect.flatMap(_.toSeq)
    import sqlContext.implicits._
    val byInstArray = tmp_5.map(blk => {

      var tmpin = tmp_4_b.filter(tmp_4_b("inst") === blk)
           tmpin.show(60)
      val pipe = SparkUtils.arima_model(tmpin)


      SparkUtils.saveModel(sc, pipe, blk.toString)
    }

    )
  }

  def trns(df: DataFrame): DataFrame = {


    //val inputDF = sqlContext.read.parquet("/home/bju/data_app/data_EM/data_2/")
//    val inputDF = hc.read.parquet("/home/bju/data_app/data_EM/data_2/")
    import org.apache.spark.sql.functions._

    val udf_tm = udf {
      dat: String =>
        val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dat)
        import java.text.SimpleDateFormat
        //val df = new SimpleDateFormat("yyyy-MM-dd:HH")
        val df = new SimpleDateFormat("EEE")
        val date_e = df.format(date_c)
        date_e

    }

    val udf_ddhh = udf {
      dat: String =>
        val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dat)
        import java.text.SimpleDateFormat
        val df = new SimpleDateFormat("yyyy-MM-dd:HH")
        val date_e = df.format(date_c)
        date_e

    }

    val udf_tm_mil = udf {
      dat: String =>
        val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dat).getTime
        date_c

    }

    import java.text.DateFormat
    import java.text.SimpleDateFormat

    val tmp = df.withColumn("weekday", udf_tm(col("eventTime")))
      .withColumn("milis", udf_tm_mil(col("eventTime")))
      .withColumn("ddHH", udf_ddhh(col("eventTime")))
    val wSpec1 = Window.partitionBy("inst").orderBy("milis").rowsBetween(-24, -1)
    val tmp_2 = tmp.withColumn("movingAvg_24", avg(tmp("consumed")).over(wSpec1))
    val wSpec2 = Window.partitionBy("inst").orderBy("milis").rowsBetween(-1, -1)
    val tmp_3 = tmp_2.withColumn("movingAvg_1", avg(tmp("consumed")).over(wSpec2))
    val wSpec3 = Window.partitionBy("inst").orderBy("milis").rowsBetween(-150, -1)
    val tmp_4 = tmp_3.withColumn("movingAvg_7D", avg(tmp("consumed")).over(wSpec3))
    tmp_4.na.replace(tmp_4.columns,Map("" -> "0"))

    tmp_4
  }



def Cons_12HFor(date:String,inst:String, Frame:DataFrame) = {

  val conf = new SparkConf()
    .setAppName("A4")
    .setMaster("local[*]")
    .set("spark.casandra.connection.host", "localhost")
  val sc_b = SparkContext.getOrCreate(conf)
  val sqlContext = getSQLContext(sc_b)
// val sc_b =getSparkContext("A4")
  val model =SparkUtils.loadModel(sc_b,inst)._3

  //val tmp_4 = hc.read.parquet("/home/bju/data_app/data_EM/data_2/").groupBy("eventHour","inst","eventTime","location").agg(sum("generated").alias("generated"),sum("consumed").alias("consumed"))
  val udf_tm_mil = udf {
    dat: String =>
      val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dat).getTime
      date_c

  }

  def getmil(r:Row) =  {
    dat: String =>
      val date_c = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(r.getString(1)).getTime
      date_c

  }


  import org.apache.spark.sql.functions._
  //(hour,dat, inst,"pep2",0,0)
 // ,first("eventTime").alias("eventTime")
  var tmp_5 = Frame.groupBy("inst","eventTime","location","eventHour").agg(sum("generated").alias("generated"),sum("consumed").alias("consumed")).cache()
  val tmp = tmp_5.select(col("eventHour"),col("eventTime"),col("inst"),col("location"),col("generated"),col("consumed"),udf_tm_mil(col("eventTime")).as("milis"))
  val LRecord= tmp.sort(desc("milis")).select("milis").map(r=>r.getAs[Long](0)).first()
  var date2 = LRecord

  case class FRWD_CON(
                       inst : String,
                       eventTime:String,
                       location: String,
                       eventHour:String,
                       generated: Double,
                       consumed: Double
                     )
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.sql.{DataFrame, Row}


  var c:Double=0
  var sev = Seq()
  val j = for(i<-0 until 24)yield {
    date2 = date2+(60*60*1000)
    var format = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    var dat = format.format(date2)
    var format_2 = new SimpleDateFormat("HH")
    var hour = format_2.format(date2)
    //var secCon= sev :+ (inst,dat,"pepe2",hour,("0").toDouble,("0").toDouble)
    var secCon= Seq((inst,dat,"pepe2",hour,("0").toDouble,("0").toDouble))
    import sqlContext.implicits._
    var tmp_g = sc_b.parallelize(secCon).toDF()
    tmp_5 = tmp_5.unionAll(tmp_g)

    //var df2= trns(tmp_5.unionAll(tmp_g))
    var df2= trns(tmp_5)
    var thresh = (System.currentTimeMillis()-60*60*24*10*1000)
    var df3= df2.filter(df2("milis") >= thresh)

    var df7 = df3.selectExpr("cast(eventHour as double) eventHour ",
      "weekday",
      "cast(movingAvg_24 as double) movingAvg_24",
      "cast(consumed as double)   consumed",
      "cast(movingAvg_1 as double)  movingAvg_1 ",
      "cast(movingAvg_7D as double)  movingAvg_7D",
      "eventTime",
      "inst"

    )
    //df7.show(5000)
    var df4  = model.transform(df7)
    //df4.show(200)
    c = df4.filter(df4("eventTime")===dat).select("prediction").map(r=>r.getDouble(0)).first()
    df4.show(500)
    FRWD_CON(inst,dat,"pep2",hour,0,c)



  }


  val d = j.map(a=>Logs.UG_Hour(a.eventHour.toDouble,a.consumed))
  val consm_m:Map[Double,Double] =d.map(a=>(a.hora.toDouble,a.nub.toDouble)).toMap

  consm_m


}
}