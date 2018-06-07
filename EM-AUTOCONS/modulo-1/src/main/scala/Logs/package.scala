import org.apache.spark.rdd.RDD

package object Logs {
  case class LogHH(
                   insta: String,
                   location: String,
                   format: String,
                   consumed: Long,
                   batered: Long,
                   generated: Long,
                   inputProps: Map[String, String] = Map()
                     )

  case class LogHHB(inst: String,
                    location: String,
                    eventTime: String,
                    eventHour: String,
                    consumed: String,
                    generated:String
                   )

  case class HH_Agg(
                         consumed: Long,
                         generated:Long
                       )


  case class HH_Agg_Akk(
                    consumed: Long,
                    generated:Long,
                    counter: Long
                   )

  case class HH_Agg_DB_Akk(
                        inst: String,
                        eventtime: String,
                        eventhour: String,
                        consumed: String,
                        generated:String,
                        counter:String
                   )


  case class HH_Agg_DB(
                            inst: String,
                            eventtime: String,
                            eventhour: String,
                            consumed: String,
                            generated:String
                          )


  case class FRD_HH(
                        inst: String,
                        gen: Map[Int,Double],
                        consm: Map[Int,Double]

                      )

  case class FRD_HH_2CSR(
                     inst: String,
                     hour: Double,
                     gen: Double,
                     consm: Double,
                     bat_acc: Double,
                     excess: Double,
                     bat: Double,
                     pool: Double

                   )



  case class state_h(
                       bat_acc_t: Double,
                       excess: Double,
                       bat: Double,
                       pool: Double

                     )

  case class FRD_HH_B(

                       estados: RDD [(String,Double,Double,Double, state_h)]

                     )
  case class STF_Hour(
                       hora: String,
                       nub: String

                     )

  case class UG_Hour(
                      hora: Double,
                      nub: Double

                    )

  case class inst_str(
                     inst:String,
                     hour:String

                     )




}
