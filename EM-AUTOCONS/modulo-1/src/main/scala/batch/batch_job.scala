package batch

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.sql.Blob
import java.text.SimpleDateFormat

import com.datastax.spark.connector.SomeColumns
import config.Settings
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, udf, when}
import org.apache.spark.{SparkConf, SparkContext}
import utils.SparkUtils.{getSQLContext, getSparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.SparkUtils
import org.apache.spark.sql.SaveMode

//Este archivo es para lanzar pruebas, se incia desde batch_jobsII, o desde gen_model en lambda
object batch_job{


  def start = {


   // gen_model.start


    //val tmp_4 = hc.read.parquet("/home/bju/data_app/data_EM/data_2/")

    //consum_mods.trns(tmp_4).show(50)
    //tmp.foreach(a=>
     // a._2.transform()
     // println(a))

    //val model = utils.SparkUtils.arima_model(tmp_4)

    //tmp_4.show(100)


  }
}