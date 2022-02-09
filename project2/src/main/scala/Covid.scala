import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import org.apache.hadoop.hive.ql.metadata.Hive

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

      Bottom10DeathRates(hiveCtx)
      insertCovidData(hiveCtx)


  }

    def insertCovidData(hiveCtx:HiveContext): Unit = {
                //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/covid_19_data.txt' OVERWRITE INTO TABLE data1")
        //hiveCtx.sql("INSERT INTO data1 VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

        // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
        // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can 
        // then be 
        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/covid_19_data.csv")
        output.limit(15).show() // Prints out the first 15 lines of the dataframe

        // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 

        // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
        // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this 
        // code as well as the creation of output will not be necessary.
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS covid1 (SNo INT, ObservationDate STRING, Province_State STRING, Country_Region STRING, LastUpdate STRING, Confirmed INT, Deaths INT, Recovered INT)")
        hiveCtx.sql("INSERT INTO covid1 SELECT * FROM temp_data")
        
        // To query the data1 table. When we make a query, the result set ius stored using a dataframe. In order to print to the console, 
        // we can use the .show() method.
        val summary = hiveCtx.sql("SELECT * FROM covid1 LIMIT 10")
        summary.show()
    }

    def Bottom10DeathRates(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT Province_State State, MAX(deaths)/MAX(confirmed) Death_Rate FROM covid1 WHERE country_region='US' AND Province_State NOT LIKE '%,%' GROUP BY State ORDER BY Death_Rate ASC LIMIT 10")
        result.show()
        result.write.csv("results/top10DeathRates")
    }
}