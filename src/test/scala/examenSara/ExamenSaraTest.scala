package examenSara

import examenSara.ExamenSara._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import utils.TestInit



class ExamenSaraTest extends FlatSpec with Matchers with TestInit {

  import spark.implicits._
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

  "ejercicio1" should "filter and order df students" in {



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

  "ejercicio2" should "Mark if a column has odd or even numbers" in {

    val datos = Seq(1, 2, 3, 4, 5)
    val df = datos.toDF("miColumna")

    val resultado = ejercicio2(df, "miColumna")(spark)

    val results = resultado.collect()
    results(0).getAs[String]("par o impar") shouldBe "impar"
    results(1).getAs[String]("par o impar") shouldBe "par"
    results(2).getAs[String]("par o impar") shouldBe "impar"
    results(3).getAs[String]("par o impar") shouldBe "par"
    results(4).getAs[String]("par o impar") shouldBe "impar"

  }
  "ejercicio3" should "Join two dataframes on Student id, and calculate the average grade per student" in {
    // Testing data
    val studentName = Seq(
      (1, "María"),
      (2, "José"),
      (3, "Guillermo"),
      (4, "Andrea"),
      (5, "Alejandro"),
      (6, "Martín")
    ).toDF("id", "Name")
    val grades = Seq(
      (1, "Matemáticas", 8.5),
      (1, "Física", 7.2),
      (1, "Historia", 6.8),
      (2, "Matemáticas", 9.1),
      (2, "Química", 8.4),
      (2, "Literatura", 7.5),
      (2, "Biología", 6.9),
      (3, "Física", 8.0),
      (3, "Historia", 7.7),
      (3, "Geografía", 6.5),
      (4, "Matemáticas", 7.8),
      (4, "Química", 8.2),
      (4, "Biología", 7.1),
      (4, "Literatura", 6.9),
      (4, "Filosofía", 9.0),
      (5, "Historia", 8.5),
      (5, "Geografía", 7.3),
      (5, "Economía", 6.8),
      (5, "Arte", 9.2),
      (6, "Matemáticas", 9.5),
      (6, "Física", 8.8),
      (6, "Programación", 9.7)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    //Ejecucion
    val result = ejercicio3(studentName, grades)
    val esperado = Seq(
      (1, "María", 7.5),
      (2, "José", 7.975),
      (3, "Guillermo", 7.4),
      (4, "Andrea", 7.8),
      (5, "Alejandro", 7.95),
      (6, "Martín", 9.33)
    ).toDF("id", "Name", "nota_media")

    val res = result.collect().sortBy(_.getInt(0))
    val exp = esperado.collect().sortBy(_.getInt(0))

    res.zip(exp).foreach { case (r, e) =>
      r.getInt(0) shouldBe e.getInt(0)
      r.getString(1) shouldBe e.getString(1)
      math.abs(r.getDouble(2) - e.getDouble(2)) should be < 0.01
    }

  }
}
