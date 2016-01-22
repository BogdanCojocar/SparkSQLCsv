package home.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Locale

object SparkSQLCsv {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkSQLCsv")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlCt = new SQLContext(sc)

    val data = sc.textFile("file:///D:/Programming/workspace/SparkSQLCsv/src/main/resources/data.csv")
		
		val header = data.first();
		val cleanData = data.filter(line => !line.equals(header))

		val pers = cleanData.map(line => {
			val fields = line.split(",")			
			val age = fields(2).trim.toInt
			
			val dformat = new SimpleDateFormat("dd/mm/yyyy", Locale.ENGLISH)
			val date = dformat.parse(fields(3).trim)
			
		  new Person(fields(0).trim, fields(1).trim, age, date.getTime())
		})
		
		val df = sqlCt.createDataFrame(pers, classOf[Person])
		df.registerTempTable("persons")

		println("Male people: " + df.filter("sex='male'").count())
		
		val row = sqlCt.sql("SELECT AVG(age) FROM persons").collect()(0)
		println("Average age: " + row.getDouble(0) * 365)
		
		val jeff = df.filter("fullName='Jeff Briton'").collect()(0)
		val tom = df.filter("fullName='Tom Soyer'").collect()(0)
		
		val ms = Math.abs(jeff.getLong(1) - tom.getLong(1))
		val days = (ms / (1000*60*60*24))
		println("Year difference in days: " + days)
		
		//sc.close
  }
}