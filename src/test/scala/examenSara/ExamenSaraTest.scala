package examenSara

import examenSara.ExamenSara._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import utils.TestInit



class ExamenSaraTest extends FlatSpec with Matchers with TestInit {

  import spark.implicits._
  //Datos de prueba
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
  // Convierte a DataFrame
  val dfStudents = studentsData.toDF("name", "age", "grade")

  "ejercicio1" should "filtrar y ordenar listado de estudiantes con notas superiores a 8" in {

    val res = ejercicio1(dfStudents)

    //Comprueba estructura del DF res
    res.schema shouldBe StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("grade", DoubleType, nullable = false)
    ))

    res.show()

    //Verifica los valores específicos de los primero 3 registros
    val collectedResults = res.collect()
    collectedResults.take(3) should contain allOf(
      Row("Lucía", 9.9),
      Row("María", 9.5),
      Row("Sofía", 9.1)
    )
  }

  "ejercicio2" should "Indica si un numero es par o impar" in {

    val datos = Seq(1, 2, 3, 4, 5)
    val df = datos.toDF("miColumna")

    val resultado = ejercicio2(df, "miColumna")(spark)

    val resultados = resultado.collect()
    resultados(0).getAs[String]("par o impar") shouldBe "impar"
    resultados(1).getAs[String]("par o impar") shouldBe "par"
    resultados(2).getAs[String]("par o impar") shouldBe "impar"
    resultados(3).getAs[String]("par o impar") shouldBe "par"
    resultados(4).getAs[String]("par o impar") shouldBe "impar"

  }
  "ejercicio3" should "Join dos dataframes por Student id y calcula la nota media por alumno" in {
    // Datos
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

    //Ejecución
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
  "ejercicio4" should "contar el número de veces que aparece cada palabra de una lista" in {

    // Lista de palabras de ejemplo
    val palabras = List("gato", "perro", "gato", "pez", "perro", "perro", "loro")

    val resultado = ejercicio4(palabras)(spark)

    // Resultado esperado
    val esperado = Map(
      "gato" -> 2,
      "perro" -> 3,
      "pez" -> 1,
      "loro" -> 1
    )

    // Comparación de los resultados, convirtiendo el RDD a Map
    resultado.collect().toMap shouldBe esperado
  }


  "ejercicio5" should "Pasa de un archivo .csv a DF y calcula el precio_unitario" in {
    val dfVentas:DataFrame = spark.read
      .option("header", "true")
      .csv("/Users/saracarcamo/Documents/KeepCoding/Practicas/BD_Proc/BD_Processing_Practica/.idea/csv/ventas.csv")

    val result = ejercicio5(dfVentas)(spark)
   // Verifica todos los headers
    result.schema.fields.map(_.name) should contain allOf ("id_venta", "id_producto", "cantidad", "precio_unitario", "ingreso_total")

    // verifica  el primer valor
    val check = result.collect()
    check(0).getAs[Double]("ingreso_total") shouldBe check(0).getAs[Int]("cantidad") * check(0).getAs[Double]("precio_unitario")
  }
}
