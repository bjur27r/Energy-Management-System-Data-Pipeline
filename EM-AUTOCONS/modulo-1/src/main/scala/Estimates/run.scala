package Estimates

import Logs.UG_Hour
import org.apache.spark.rdd.RDD
import sun.nio.cs.ext.DoubleByteEncoder

//Este paquete incluye  funciones para la previsión energética , en una segunda fase de optimización en función del precio del pool

object run {


    def charge_bat(gen:Double,consm:Double,pool_price:Double, bat_status:String,bat_acc:Double,charge:Boolean, charge_pool:Boolean)={
      var bat = 0
      var pool = 0
      var bat_acc_t = 0
      val exc = (consm>gen)
      var tmp = consm - gen - bat_acc
      val tup = (exc,tmp>0 ,bat_status)

      tup match{
        case(true,true,"N")=>Logs.state_h(0,0,0,tmp)//Hay Exceso consumo
        case(true, true,"C")=>Logs.state_h(0,0,bat_acc ,consm-gen)//Hay Exceso No cubre Bat porque está cargando
        case(true,false,"N")=>Logs.state_h((bat_acc-consm + gen),0,(consm - gen),0)
        case(true, false, _)=>Logs.state_h(0,0,bat_acc ,tmp)
        case(true,_,"C")=>Logs.state_h(0,0,bat_acc ,(consm - gen))
        case(false,true,"N")=>Logs.state_h((bat_acc+gen-consm),(gen-consm),(+gen-consm),0)
        case (false, false, "C")=>Logs.state_h((bat_acc+gen-consm),(gen-consm),(+gen-consm),0)
        case (false, false, "N")=>Logs.state_h((bat_acc+gen-consm),(gen-consm),(+gen-consm),0)
        case (_, _, _)=>Logs.state_h(0,(gen-consm),(bat_acc+gen-consm),0)


      }
    }


    def HourFordward(insta: String,gen:RDD[Logs.UG_Hour],consm_m:Map[Double,Double]) = {


      var bat_acc:Double = 0
      val dd_2:RDD[(String,Double,Double,Double,Logs.state_h)] = gen.map(a=>{
        //bat_acc + charge_bat(a.nub,a.hora.toDouble,0.13, "N",bat_acc,false, false).bat
        var az  = (insta,a.hora,a.nub,consm_m(a.hora.toDouble),charge_bat(a.nub,consm_m(a.hora.toDouble),0.13, "N",bat_acc,false, false))
        bat_acc = charge_bat(a.nub,consm_m(a.hora.toDouble),0.13, "N",bat_acc,false, false).bat_acc_t
        az
      }
      )
      val bat_2 = Logs.FRD_HH_B(dd_2)
      bat_2

    }




}
