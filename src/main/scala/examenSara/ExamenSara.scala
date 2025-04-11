package examenSara

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
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

  def ejercicio1(students:DataFrame)(implicit spark:SparkSession): DataFrame = {
    // Muestra el esquema del df.
    students.printSchema()
    // Filtra calificaciones > 8, selecciona nombres y ordena descendente

    val resStudents = students
      .filter(col("grade") > 8)
      .select("name", "grade")
      .orderBy(desc("grade"))

    resStudents
  }
  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame, columnName: String)(spark: SparkSession): DataFrame = { // Quise añadirle la variable para el nombre de la columna
    // Función que mira a cada valor y determina si es par o impar
    val parOImpar = udf((num:Int) => num % 2 match {
      case 0 => "par"
      case _ => "impar"
    })

    // Le pasan una columna de un DF con una lista de números
    val dfPoI = numeros.withColumn("par o impar", parOImpar(col(columnName)))

    dfPoI
  }
  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dado dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._

    //join entre los dos dataFrames en el id del alumno
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    val dfJoined = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"), "left_outer")
      .select(
        estudiantes("id"),
        estudiantes("Name"),
        calificaciones("calificacion"))

    //agrupar por student_id y calcular la media de notas
    val StudentGrades = dfJoined
      .groupBy("id", "Name")
      .agg(round(avg("calificacion"), 2).as("nota_media"))
      .orderBy("id")
    StudentGrades

  }
  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   *
   */

  def ejercicio4(palabras: List[String])(spark: SparkSession): RDD[(String, Int)] = {
    val rddPalabras = spark.sparkContext.parallelize(palabras)
    //count how many times each word appear
    val tuplasPalabras: RDD[(String, Int)] = rddPalabras.map(palabra => (palabra, 1))
    val conteo: RDD[(String, Int)] = tuplasPalabras.reduceByKey(_ + _)
    conteo
  }

  /**
   * Ejercicio 5: Procesamiento de archivos
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(spark: SparkSession): DataFrame = {

    // Asegurarse de que las columnas de cantidad y precio_unitario sean del tipo correcto
    val dfVentasTyped = ventas
      .withColumn("cantidad", col("cantidad").cast("int")) // Convertir a tipo Integer
      .withColumn("precio_unitario", col("precio_unitario").cast("double")) // Convertir a tipo Double

    // Calcular el ingreso total
    val dfVentasConIngreso = dfVentasTyped.withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))

    dfVentasConIngreso
  }




}
