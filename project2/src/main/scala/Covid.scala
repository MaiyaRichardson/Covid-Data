import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

// second changes made here

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


        insertCovidData(hiveCtx)
        top10CasesUS(hiveCtx)
        btm10CasesUS(hiveCtx)
        top10CasesByDate(hiveCtx)
        btm10CasesByDate(hiveCtx)
      
        Bottom10ConfirmedByContinent(hiveCtx)
        Top10ConfirmedByLocation(hiveCtx)
        Top10DeathsUSbyDate(hiveCtx)

        var scanner = new Scanner(System.in)
        var choice = 0
        while (choice != 11){
            try {
                println("Please choose an option using number keys 1-11")
                println("1.) Top 10 deaths by country")
                println("2.) Bottom 10 deaths by country")
                println("3.) Top 10 deaths by states in US (partition & bucket)")
                println("4.) Bottom 10 deaths by states in US (partition & bucket)")
                println("5.) Top 10 confirmed cases by country")
                println("6.) Bottom 10 confirmed cases by country")
                println("7.) Top 10 confirmed cases in US (partition & bucket)")
                println("8.) Bottom 10 confirmed cases in US (partition & bucket)")
                println("9.) Top 10 Confirmed cases by (05/02/2021) by country")
                println("10.) Bottom 10 confirmed cases  by (05/02/2021) by country")
                println("11.) Quit")
                var choice =  scanner.nextInt()
                
                if (choice == 1){
                    Top10Confirmed(hiveCtx)
                    
                }
                
                

            }catch {
                case e: Exception => println("Eception thrown - Non numeric key entered")
            }
        }
        
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
        //hiveCtx.sql("DROP TABLE IF EXISTS covid1")
        //hiveCtx.sql("CREATE TABLE IF NOT EXISTS covid1 (iso_code STRING,continent STRING,location STRING,date STRING,total_cases DOUBLE,new_cases DOUBLE,total_deaths DOUBLE,new_deaths DOUBLE,new_tests DOUBLE,total_tests DOUBLE,total_vaccinations DOUBLE,people_vaccinated DOUBLE,people_fully_vaccinated DOUBLE,population INT,population_density FLOAT,median_age FLOAT,aged_65_older FLOAT,aged_70_older FLOAT,gdp_per_capita FLOAT,hospital_beds_per_thousand FLOAT,life_expectancy FLOAT)")
        //hiveCtx.sql("INSERT INTO covid1 SELECT * FROM temp_data")
        val summary = hiveCtx.sql("SELECT continent, location, total_cases FROM covid1 LIMIT 10")
        summary.show()
        //val summary2 = hiveCtx.sql("SELECT to_date(('date'),'MM/dd/yyyy') date FROM covid1 LIMIT 10") 
        
        //summary2.show()

    }
    

    //Maiya
    def Top10DeathsUSbyDate(hiveCtx:HiveContext): Unit = {

      
        val result = hiveCtx.sql("SELECT location, date, total_deaths FROM covid1 WHERE location = 'United States' ORDER BY total_deaths DESC LIMIT 10")
        println("Top10DeathsUSByDate")
        result.show()

        
    }
    //Maiya
    def Bottom10ConfirmedByContinent(hiveCtx:HiveContext): Unit = {
        

        val result = hiveCtx.sql("SELECT location, date, MIN(total_cases) AS MinimumTotalCases FROM covid1 WHERE total_cases >= 0.0 GROUP BY location, date ORDER BY MinimumTotalCases ASC LIMIT 30")
        println("\n Bottom 10 confirmed cases by continent\n")
        result.show()
        //result.write.csv("results/Bottom10DeathRatesByStatesInUS")
    }
    //Maiya
    def Top10ConfirmedByLocation(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT location, date, MAX(total_cases) AS MaximumTotalCases FROM covid1 GROUP BY location, date  ORDER BY MaximumTotalCases DESC LIMIT 10")
        println("Top 10 confirmed cases by location \n")
        result.show()
        //result.write.csv("results/Top10ConfirmedByContinent")
    }

    def Top10Confirmed(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT continent, MAX(total_cases) Confirmed_Cases FROM covid1 GROUP BY continent ORDER BY Confirmed_Cases ASC LIMIT 10")
        println("Top 10 confirmed in the world '\n'")
        result.show()
        result.write.csv("results/Top10ConfirmedByContinent")
    }

    // changed by wakgari
    def top10DeathsByContinent(hiveCtx:HiveContext): Unit = {
        println("===Print top 10 death by continent===")
        val result = hiveCtx.sql("SELECT continent, MAX(total_deaths) Total_Deaths FROM covid1 WHERE GROUP BY continent ORDER BY Total_Deaths DESC LIMIT 10")
        result.show()
        result.write.csv("results/top10DeathsByContinent")
    }

    // Wakgari
    def top10CasesByContinent(hiveCtx:HiveContext): Unit = {
        println("==Print top 10 death by country==")
        val result = hiveCtx.sql("SELECT DISTINCT continent, MAX(total_cases) Max_cases from covid1 GROUP BY continent ORDER BY Max_cases DESC LIMIT 10")
        result.show()
        result.write.csv("results/top10CasesByContinent")
    }

    // Wakgari
    def maxAndMinTotalVaccination(hiveCtx:HiveContext): Unit = {
        println("===Print MIN total vaccinations and MAX total vaccinations===")
        val result = hiveCtx.sql("SELECT continent, MAX(total_vaccinations) AS Max_Total_vaccination from covid1 GROUP BY continent ORDER BY Max_Total_vaccination DESC")
        result.show()
        result.write.csv("results/maxAndMinTotalVaccination")
       
    }
    //Sharyar
    def top10CasesUS(hiveCtx:HiveContext): Unit = {
        println("===Top 10 confirmed cases in US===")
        val result = hiveCtx.sql("SELECT location, date, MAX(total_cases) AS Max_Total_Cases from covid1 WHERE location = 'United States' GROUP BY location, date ORDER BY Max_Total_Cases DESC LIMIT 10")
        result.show()
        result.write.mode("overwrite").csv("results/top10CasesUS")
       
    }
    //Sharyar
    def btm10CasesUS(hiveCtx:HiveContext): Unit = {
        println("===Bottom 10 confirmed cases in US===")
        val result = hiveCtx.sql("SELECT location, date, Min(total_cases) AS Min_Total_Cases from covid1 WHERE location = 'United States' GROUP BY location, date ORDER BY Min_Total_Cases ASC LIMIT 10")
        result.show()
        result.write.mode("overwrite").csv("results/btm10CasesUS")
       
    }
    def top10CasesByDate(hiveCtx:HiveContext): Unit = {
        println("===Top 10 Confirmed cases by (5/2/2021) by country===")
        val result = hiveCtx.sql("SELECT location, date, Max(total_cases) AS Max_Total_Cases from covid1 WHERE date = '5/2/2021' GROUP BY date, location ORDER BY Max_Total_Cases DESC LIMIT 10")
        result.show()
        result.write.mode("overwrite").csv("results/top10CasesByDate")
    }
    def btm10CasesByDate(hiveCtx:HiveContext): Unit = {
        println("===Top 10 Confirmed cases by (5/2/2021) by country===")
        val result = hiveCtx.sql("SELECT location, date, Min(total_cases) AS Min_Total_Cases from covid1 WHERE date = '5/2/2021' AND total_cases >= 0 GROUP BY date, location ORDER BY Min_Total_Cases ASC LIMIT 10")
        result.show()
        result.write.mode("overwrite").csv("results/top10CasesByDate")
    }

}