package examenSara

import examenSara.ExamenSara._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import utils.TestInit

class ExamenSaraTest extends TestInit {

  "DataFrame" should "Read" in {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("grade", DoubleType, nullable = false)
    ))

    val datos = Seq(
      Row("Lucía", 22, 9.9),
      Row("Diego", 19, 6.0)
    )

    val df: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(datos),
      schema
    )

    df.show()
    df.printSchema()

  }

}
