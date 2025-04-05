package sesion4.spUtils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def runSparkSession(name: String): SparkSession = SparkSession.builder() //Arrancar una sesion de Spark.
    .master("local[*]")
    .appName(name)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

}