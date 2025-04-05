import org.apache.spark.sql.SparkSession
import sesion4.spUtils.SparkUtils.runSparkSession

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = runSparkSession("KeepCoding") //Para iniciar Spark una vez

    val df = spark.read.csv("/Users/saracarcamo/Documents/KeepCoding/Modulos_Bootcamp/Big_Data_Processing/Big-Data-Processing-main_II/examen/ventas.csv")

    df.show(false)
   //** args match {
   //   case Seq("Sesion4") => f3
    //  case Seq("Examen") => f2
    spark.stop



  }
}