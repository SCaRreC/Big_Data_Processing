package examenSara

import examenSara.ExamenSara._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import utils.TestInit

class ExamenSaraTest extends TestInit {

  import spark.implicits._

  "ejercicio1" should "filter and order df students" in {

    //Crear datos de prueba
    val studentsData = Seq(
      ("Juan", 20, 7.5),
      ("María", 22, 9.5),
      ("Carlos", 20, 8.3),
      ("Ana", 19, 7.8),
      ("Luis", 21, 6.5),
      ("Sofía", 23, 9.1),
      ("Javier", 18, 5.9),
      ("Elena", 24, 8.7),
      ("Pedro", 20, 7.2),
      ("Lucía", 22, 9.9),
      ("Diego", 19, 6.0)
    )
    // Convertir a DataFrame
    val dfStudents = studentsData.toDF("name", "age", "grade")

    //Ejecucion

    dfStudents.schema shouldBe StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = false),
      StructField("grade", DoubleType, nullable = false)
    ))

    val res = ejercicio1(dfStudents)

    res.schema shouldBe StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("grade", DoubleType, nullable = false)
    ))

    res.show()
    // Verificar el ordenamiento descendente
    val collectedResults = res.collect()
    //val grades = collectedResults.map(_.getAs[Double]("grade"))
    //grades shouldBe grades.sorted.reverse

    // Verificar los valores específicos (primeros 3)
    collectedResults.take(3) should contain allOf(
      Row("Lucía", 9.9),
      Row("María", 9.5),
      Row("Sofía", 9.1)
    )

  }

}
