package config




import com.typesafe.config.ConfigFactory


object Settings {

  private val config = ConfigFactory.load()

  object HHLogGen {
    private val hhlogGen = config.getConfig("HHIoTSignalStream")
    lazy val inst = hhlogGen.getInt("installations")
    lazy val pot_gen = hhlogGen.getDouble("pot_gen")
    lazy val timeMultiplier = hhlogGen.getInt("time_multiplier")
    lazy val records = hhlogGen.getInt("records")
    lazy val filePath = hhlogGen.getString("file_path")
    lazy val destPath = hhlogGen.getString("dest_path")
    lazy val numberOfFiles = hhlogGen.getInt("number_of_files")
    lazy val kafkaTopic = hhlogGen.getString("kafka_topic")
    lazy val hdfsPath = hhlogGen.getString("hdfs_path")

  }
}
