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
        
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS covid1 (iso_code INT,continent INT,location INT,date INT,total_cases INT,new_cases INT,total_deaths INT,new_deaths INT,new_tests INT,total_tests INT,total_vaccinations INT,people_vaccinated INT,people_fully_vaccinated INT,population INT,population_density INT,median_age INT,aged_65_older INT,aged_70_older INT,gdp_per_capita INT,hospital_beds_per_thousand INT,life_expectancy INT)")
        hiveCtx.sql("INSERT INTO covid1 SELECT * FROM temp_data")
        val summary = hiveCtx.sql("SELECT * FROM covid1 LIMIT 10")
        summary.show()
     
    }

    def Top10DeathRatesUS(hiveCtx:HiveContext): Unit = {
        
      
        hiveCtx.sql("DROP TABLE IF EXISTS covid2")
    

        val summary = hiveCtx.sql("Select * FROM covid2 WHERE Country_Region = 'US' LIMIT 20")
        summary.show()
    }

    def Bottom10DeathRates(hiveCtx:HiveContext): Unit = {
        

        val result = hiveCtx.sql("SELECT Province_State State, MAX(deaths)/MAX(confirmed) Death_Rate FROM covid1 WHERE country_region='US' GROUP BY State ORDER BY Death_Rate ASC LIMIT 10")
        println("\n Bottom 10 Death Rates in US \n")
        result.show()
        //result.write.csv("results/top10DeathRatesByStatesInUS")
    }

    def Top10Confirmed(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT country_region, Confirmed FROM covid1 GROUP BY country_region ASC LIMIT 10")
        println("Top 10 confirmed in the world '\n'")
        result.show()
        result.write.csv("results/top10confirmedbycountry")
    }
}