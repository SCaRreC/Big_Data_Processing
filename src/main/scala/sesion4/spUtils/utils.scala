package sesion4.spUtils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object utils {
  def writeCsv(df:DataFrame, path:String): Unit = df.write.option("header", true).mode("overwrite").csv(path)


  def lecturaCsv(path: String)(sc: SparkContext): RDD[String] = sc.textFile(path)

  def lecturaCsvDf(path: String, delimiter: String=",")(implicit spark: SparkSession): DataFrame =
    spark.read
      .options(Map(("header", "true"), ("delimiter", delimiter)))
      .csv(path)

  def lecturaCsvDf(path: String, options: Map[String, String])
                  (implicit spark: SparkSession): DataFrame =
    spark.read
      .options(options)
      .csv(path)


}