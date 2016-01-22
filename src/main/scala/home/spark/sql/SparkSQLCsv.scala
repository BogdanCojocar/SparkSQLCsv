package home.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Locale
import java.lang.String
import org.apache.spark.sql.DataFrame

case class Data(fullName: String, sex: String, age: Integer, dob: Long)

object SparkSQLCsv {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkSQLCsv")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlCt = new SQLContext(sc)

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
		
    parsedData.foreach(println)
		val df = sqlCt.createDataFrame(parsedData)
		df.registerTempTable("data")

    df.printSchema()
		println("Male people: " + df.filter("sex='male'").count())
		
		val row = sqlCt.sql("SELECT AVG(age) FROM data").collect()(0)
		println("Average age: " + row.getDouble(0) * 365)
		
		val jeff = df.filter("fullName='Jeff Briton'").collect()(0)
		val tom = df.filter("fullName='Tom Soyer'").collect()(0)
		
    println(jeff)
		val ms = Math.abs(jeff.getLong(3) - tom.getLong(3))
		val days = (ms / (1000*60*60*24))
		println("Year difference in days: " + days)
		
		//sc.close
  }
}