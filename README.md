# Big Data Processing con Apache Spark 

Este repositorio contiene la resolución de una serie de ejercicios prácticos realizados durante el módulo de **Big Data Processing** del programa de formación en Big Data, Machine Learning & AI. El objetivo principal ha sido aprender a crear una sesión de Spark, manejar estructuras de datos distribuidas y aplicar transformaciones y acciones sobre DataFrames y RDDs utilizando **Apache Spark** en **Scala**.

##  Objetivo del proyecto

- Conectar correctamente con Spark y crear una `SparkSession` funcional.
- Aplicar operaciones básicas y avanzadas sobre DataFrames y RDDs.
- Utilizar funciones de transformación (`select`, `filter`, `join`, `withColumn`, etc.).
- Leer y procesar archivos CSV en Spark.
- Consolidar conocimientos adquiridos a través de la resolución de 5 ejercicios prácticos.

##  Ejercicios incluidos

1. **Filtrado y ordenación de DataFrames:** Selección de estudiantes con notas superiores a 7.5 y ordenamiento descendente.
2. **Etiquetado de valores impares y pares:** Creación de nuevas columnas condicionales.
3. **Cálculo de nota media por estudiante:** Join entre múltiples DataFrames y agrupaciones.
4. **Conteo de palabras con RDDs:** Ejemplo clásico de procesamiento distribuido.
5. **Lectura y procesamiento de archivos CSV:** Cálculo del ingreso total por producto a partir de un archivo CSV de ventas.

##  Tecnologías y herramientas

- Apache Spark 3.x
- Scala
- SBT
- IntelliJ IDEA
- Scalatest para pruebas unitarias

##  Testing

Cada ejercicio está acompañado por pruebas unitarias escritas con **Scalatest**, que permiten verificar el correcto funcionamiento del código de forma modular y automatizada.
