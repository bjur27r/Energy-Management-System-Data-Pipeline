package utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.{SparkConf, SparkContext}
import java.lang.management.ManagementFactory

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Duration, StreamingContext}


object SparkUtils {

//Checkeo entorno
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  //Get Spark Contexts
  def getSparkContext(appName: String)={
    var checkpointDirectory = ""

    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.casandra.connection.host", "localhost")

    if (!isIDE) {
      System.setProperty("hadoop.home.dir", "/home/bju/data_app/data")
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///home/bju/data_app/temp/"
    }else {
      checkpointDirectory = "hdfs://TFM:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc


  }

  def getSQLContext(sc:SparkContext)={
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext

  }

  //Construcción Pipeline Previsión Generación
  def features_eng(df:DataFrame):DataFrame={
    val df2 = df.selectExpr("cast(hora as double) hora",
      "nub"
    )

    val tmp_1 = new StringIndexer()
      .setInputCol("nub").setOutputCol("nub_cod")
      .fit(df2)

    val tmp_2 = tmp_1.transform(df2)

    val encoder = new OneHotEncoder()
      .setInputCol("nub_cod")
      .setOutputCol("nub_cod_b")
    val encoded = encoder.transform(tmp_2)
    val encoder_2 = new OneHotEncoder()
      .setInputCol("hora")
      .setOutputCol("hora_b")
    val encoded_2 = encoder_2.transform(encoded)
    val assembler = new VectorAssembler()
      .setInputCols(Array("nub_cod_b", "hora_b"))
      .setOutputCol("features")
    val output = assembler.transform(encoded_2)
    output

  }

  def update_model(sc:SparkContext)={
    val conf = new SparkConf()
      .setAppName("pruebas")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = getSQLContext(sc)
    val path = "/home/bju/data_app/trainig_gen.json"
    val df =  sqlContext.read.json(path)
    val df2 = df.selectExpr("cast(hora as double) hora",
      "nub",
      "out"
    )

    val tmp_1 = new StringIndexer()
      .setInputCol("nub").setOutputCol("nub_cod")
      .fit(df2)

    //val tmp_2 = tmp_1.transform(df2)

    val encoder = new OneHotEncoder()
      .setInputCol("nub_cod")
      .setOutputCol("nub_cod_b")
  //  val encoded = encoder.transform(tmp_2)
    val encoder_2 = new OneHotEncoder()
      .setInputCol("hora")
      .setOutputCol("hora_b")
   // val encoded_2 = encoder_2.transform(encoded)
    val assembler = new VectorAssembler()
      .setInputCols(Array("nub_cod_b", "hora_b"))
      .setOutputCol("features")
  //  val output = assembler.transform(encoded_2)
//    output.show(10)
    val lr = new LinearRegression()
      .setLabelCol("out")
      .setMaxIter(1000)
      .setRegParam(0.3)
      .setElasticNetParam(0.01)

    val pipeline = new Pipeline()
      .setStages(Array(tmp_1,encoder,encoder_2,assembler,lr))

      //  pipeline

    val lrModel = pipeline.fit(df2)
    lrModel
  }

  //Construcción Pipeline Previsión de Consumo

  def arima_model(df: DataFrame)={

     val df2 = df.selectExpr("cast(eventHour as double) eventHour ",
      "weekday",
      "cast(movingAvg_24 as double) movingAvg_24",
       "cast(consumed as double)   consumed",
       "cast(movingAvg_1 as double)  movingAvg_1 ",
       "cast(movingAvg_7D as double)  movingAvg_7D",
       "eventTime",
       "inst"

    ).na.drop()

    val tmp_1 = new StringIndexer()
      .setInputCol("weekday").setOutputCol("weekday_cod")
      .fit(df2)

    val encoder = new OneHotEncoder()
      .setInputCol("weekday_cod")
      .setOutputCol("weekday_cod_b")
    //  val encoded = encoder.transform(tmp_2)
    val encoder_2 = new OneHotEncoder()
      .setInputCol("eventHour")
      .setOutputCol("eventHour_b")
    // val encoded_2 = encoder_2.transform(encoded)
    val assembler = new VectorAssembler()
      .setInputCols(Array("weekday_cod_b", "eventHour_b", "movingAvg_24","movingAvg_1","movingAvg_7D"))
      .setOutputCol("features")
    //  val output = assembler.transform(encoded_2)
    //    output.show(10)
    val lr = new LinearRegression()
      .setLabelCol("consumed")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val pipeline = new Pipeline()
      .setStages(Array(tmp_1,encoder,encoder_2,assembler,lr))

    //  pipeline

    val lrModel = pipeline.fit(df2)
    lrModel
  }



  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

  //Peristir Modelo Consumo en Cassandra

  def saveModel(sc:SparkContext, model: PipelineModel, inst : String) = {
    val barray = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(barray)

      oos.writeObject(model)
    oos.flush()
    oos.close()

    import com.datastax.spark.connector._
    import com.datastax.spark.connector.streaming._

    sc.parallelize(Seq((inst,model.uid, barray.toByteArray)))
      .saveToCassandra("em","models_cons",SomeColumns("inst", "tip","model"))

  }

  def loadModel(sc:SparkContext, inst : String) = {
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.streaming._

    val arr_pip = sc.cassandraTable("em","models_cons")
    val arr_pip_2=   arr_pip.map { r =>
      var bis = new ByteArrayInputStream(r.getBytes("model").array())
      var ois = new ObjectInputStream(bis)

      (r.getString("inst"), r.getString("tip"),ois.readObject.asInstanceOf[PipelineModel])
    }.filter(a=>a._1==inst).first()
      arr_pip_2
  }



}
