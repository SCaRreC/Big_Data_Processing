package examenSara

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExamenSara {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(students: DataFrame)(implicit spark:SparkSession): Unit = {

    // Crear dataframe
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
    val dfStudents = spark.createDataFrame(studentsData).toDF("name", "age", "grade") //Pasa secuencia a dataframe
    dfStudents
      .filter("grade > 8")
      .select("name", "grade")
      .orderBy(desc("grade"))


  }


}
