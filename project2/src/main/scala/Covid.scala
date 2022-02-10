import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Covid {
  def main(args: Array[String]): Unit = {
        // This block of code is all necessary for spark/hive/hadoop

        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Covid")    // Change to whatever app name you want
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

      //Bottom10DeathRates(hiveCtx)
      insertCovidData(hiveCtx)
      //Top10Confirmed(hiveCtx)
      //Top10DeathRatesUS(hiveCtx)
      //Top10DeathsUSbyDate(hiveCtx)


  }

    def insertCovidData(hiveCtx:HiveContext): Unit = {

        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/covid-data.csv")
        //output.limit(15).show() // Prints out the first 15 lines of the dataframe

        // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 

        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("DROP TABLE IF EXISTS covid1")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS covid1 (iso_code STRING,continent STRING,location STRING,date STRING,total_cases DOUBLE,new_cases DOUBLE,total_deaths DOUBLE,new_deaths DOUBLE,new_tests DOUBLE,total_tests DOUBLE,total_vaccinations DOUBLE,people_vaccinated DOUBLE,people_fully_vaccinated DOUBLE,population INT,population_density FLOAT,median_age FLOAT,aged_65_older FLOAT,aged_70_older FLOAT,gdp_per_capita FLOAT,hospital_beds_per_thousand FLOAT,life_expectancy FLOAT)")
        hiveCtx.sql("INSERT INTO covid1 SELECT * FROM temp_data")
        val summary = hiveCtx.sql("SELECT * FROM covid1 LIMIT 10")
        summary.show()
        //val summary2 = hiveCtx.sql("SELECT to_date(('date'),'MM/dd/yyyy') date FROM covid1 LIMIT 10") 
        
        //summary2.show()
     
    }

    def Top10DeathsUSbyDate(hiveCtx:HiveContext): Unit = {
        
      
        val result = hiveCtx.sql("SELECT location, continent, total_deaths FROM covid1 WHERE iso_code = 'USA' BETWEEN CAST('7/1/2021' AS DATE) AND CAST('1/1/2022' AS DATE) ORDER BY total_deaths ASC LIMIT 20")
        println("Top10DeathsByDate")
        result.show()

        
    }

    def Bottom10ConfirmedByContinent(hiveCtx:HiveContext): Unit = {
        

        val result = hiveCtx.sql("SELECT Province_State State, MAX(total_deaths)/MAX(total_cases) Death_Rate FROM covid1 WHERE iso_code='USA' GROUP BY State ORDER BY Death_Rate ASC LIMIT 10")
        println("\n Bottom 10 confirmed by continent \n")
        result.show()
        result.write.csv("results/top10ConfirmedByContinent")
    }

    def Top10ConfirmedByContinent(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT continent, MAX(total_cases) Confirmed_Cases FROM covid1 GROUP BY continent ORDER BY Confirmed_Cases ASC LIMIT 10")
        println("Top 10 confirmed by continent \n")
        result.show()
        result.write.csv("results/Top10ConfirmedByContinent")
    }
}