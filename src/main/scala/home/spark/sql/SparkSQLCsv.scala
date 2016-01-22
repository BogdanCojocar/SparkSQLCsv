package home.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat
import java.util.Locale
import java.lang.String
import org.apache.spark.sql.DataFrame
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

case class Data(fullName: String, sex: String, age: Integer, dob: Long)
case class NewData(fullName: String, occupation: String, address: String)

object SparkSQLCsv {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkSQLCsv")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlCt = new SQLContext(sc)

    val parsedData = readDataCsv(sc)
    val newParsedData = readNewDataCsv(sc)

    // create dataframes and register the two tables
    val df = sqlCt.createDataFrame(parsedData)
    df.registerTempTable("data")

    val dfnew = sqlCt.createDataFrame(newParsedData)
    dfnew.registerTempTable("newdata")

    println("The data read from the file: ")
    parsedData.foreach(println)
    println

    println("Number of males: " + df.filter("sex='male'").count() + "\n")

    val row = sqlCt.sql("SELECT AVG(age) FROM data").collect()(0)
    println("Average age: " + row.getDouble(0) + "\n")

    println("Select name and age: ")
    val nameAndAge = sqlCt.sql("Select fullName, age FROM data")
    nameAndAge.foreach(println)
    println

    val jeff = df.filter("fullName='Jeff Briton'").collect()(0)
    val tom = df.filter("fullName='Tom Soyer'").collect()(0)

    val ms = Math.abs(jeff.getLong(3) - tom.getLong(3))
    val days = TimeUnit.MILLISECONDS.toDays(ms)
    println("Year difference in days between Jeff and Tom: " + days + "\n")

    val maxAge = sqlCt.sql("SELECT MAX(age) FROM data")
    println("Maximum age: " + maxAge.collect()(0).getInt(0) + "\n")

    println("Data ordered by age: ")
    df.orderBy(desc("age")).foreach(println)
    println

    println("Join on data and newdata tables")
    val joinData = sqlCt.sql("""
        SELECT a.fullName,
               a.sex,
               a.age,
               b.occupation,
               b.address
        FROM data a
        JOIN newdata b
        ON a.fullName = b.fullName
    """)
    joinData.foreach(println)

    sc.stop()
  }

  private def readDataCsv(sc: SparkContext): RDD[Data] = {
    val data = sc.textFile("file:///D:/SparkSQLCsv/SparkSQLCsv/src/main/resources/data.csv")

    val header = data.first();
    val cleanData = data.filter(line => !line.equals(header))

    val parsedData = cleanData.map(line => {
      val fields = line.split(",")
      val age = fields(2).trim.toInt

      val dformat = new SimpleDateFormat("dd/mm/yyyy", Locale.ENGLISH)
      val date = dformat.parse(fields(3).trim)

      Data(fields(0).trim, fields(1).trim, age, date.getTime)
    })
    parsedData
  }

  private def readNewDataCsv(sc: SparkContext): RDD[NewData] = {
    val data = sc.textFile("file:///D:/SparkSQLCsv/SparkSQLCsv/src/main/resources/newdata.csv")

    val header = data.first();
    val cleanData = data.filter(line => !line.equals(header))

    val parsedData = cleanData.map(line => {
      val fields = line.split(",")
      val fullName = fields(0).trim
      val occupation = fields(1).trim
      val address = fields(2).trim

      NewData(fullName, occupation, address)
    })
    parsedData
  }
}
