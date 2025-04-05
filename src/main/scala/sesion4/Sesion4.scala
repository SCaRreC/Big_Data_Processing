package sesion4

import org.apache.spark.rdd.RDD

object Sesion4 {

  def generarTublas(rdd: RDD[String]) = rdd.map(s => (s,1))


 }