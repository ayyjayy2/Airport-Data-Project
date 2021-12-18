/**  TCS A&I Data Science Training Group Project
 *   project: gather airport data from csv file and use timezonedb API to get the
 *      timezones of each airport given from the file
 *   API key : ZIO9EPGHURJA
 *   API Gateway : http://api.timezonedb.com
 *      http://api.timezonedb.com/v2.1/get-time-zone
 *   Query ex using long/lat:
 *    http://api.timezonedb.com/v2.1/get-time-zone?key=ZIO9EPGHURJA&format=xml&by=position&lat=40.689247&lng=-74.044502
 */

/**
 * actual project code
 * GATHER AIRPORTS OF SAME TIMEZONES/STATES
 *
 *  X : import data
 *  O : manipulate
 *    X : use csv to "Get local time of a city by its name, time zone, latitude & longtiude, or IP address"
 *    //we have a list of airport names, long, lat, municipality (city?)
 *    //the API has the city name, long/lat, and time zone
 *    //from here, use the long/lat to get the local time and time zone from API
 *      X : establish abbreviated time zones per airport
 *          X : gather airports of specific time zones or states and display them
 *      O : put the above into their own tables/files
 *  O : export to hive or some external db. Maybe HDFS is fine
 *
 */

package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, count, split}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.metrics.GarbageCollectionMetrics.names

import java.io.{File, PrintWriter}
//import org.apache.spark.sql.{SparkSession}

import scala.util.matching.Regex

object Airports {

  //Initialize a mutable map
  //var timeZoneMap = scala.collection.mutable.Map[String,Int]()

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors and not minor warnings
    Logger.getLogger("org").setLevel(Level.ERROR)
    //set home directory for windows
    System.setProperty("hadoop.home.dir", "C:/hadoop-3.2.2")

    // Use new SparkSession interface in Spark 2.0    //cannot add .enableHiveSupport()
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

    //create dataframe from airports-extended.csv file
    val extendedDf = spark.read.option("inferSchema", "true").
      schema(
        """
          |Airport_ID INT,
          |Name STRING,
          |City STRING,
          |Country STRING,
          |IATA STRING,
          |ICAO STRING,
          |Latitude DOUBLE,
          |Longitude DOUBLE,
          |Altitude INT,
          |Timezone INT,
          |DST STRING,
          |Tz_database STRING,
          |Type STRING,
          |Source STRING""".stripMargin).
      csv("data/airports-extended.csv")

    //filter out where Type=airport and where the timezone has a real value. Then select only needed columns
    val extFiltered = extendedDf.
      filter(extendedDf("Type") === "airport" && extendedDf("Tz_database") =!= "none").
      select("Name", "City", "Tz_database", "Type")

    //group by the timezone and count total num of airports in that timezone
    val numOfAirportsByTZDF = extFiltered.
        groupBy("Tz_database").
        agg(count("Tz_database").
        alias("# of Airports")).
        withColumnRenamed("Tz_database", "Time Zone").
        sort("Time Zone")

    println("displaying the numOfAirportsByTZDF dataframe : ")
    numOfAirportsByTZDF.show(15)


    //********DISPLAY DISTINCT REGIONS********
    val regionDF = numOfAirportsByTZDF.
      select(split(col("Time Zone"), "/").
      getItem(0).as("Region")).
      sort("Region").distinct()
    println("Display distinct regions")
    regionDF.show()

    //******** DISPLAY AIRPORT NAMES WITH THEIR CORRESPONDING REGIONS********
    val airportRegionsDf = extFiltered.
      select(col("Name"), col("City"), col("Tz_database").as("Time Zone"), split(col("Tz_database"), "/").
        getItem(0).as("Region")).
      sort("Region")
    println("display airport names with corresponding regions")
    airportRegionsDf.show(15)

    //export airportRegionsDF
    //****EXPORT #1******
    airportRegionsDf.write.mode("overwrite").partitionBy("Region").csv("data/airportdata")

    //********GET AIRPORT NAMES BY COUNTRY***********
    val airportByCountryDF = extendedDf.
      filter(extendedDf("Type") === "airport" && extendedDf("Tz_database") =!= "none").
      select("Country", "Name")

    println("display airports per country")
    airportByCountryDF.show(15,false)

    //count of airports per country

    //create df for zone.csv
    val zoneDf = spark.read.
      option("inferSchema", "true").
      schema("""Zone_id INT, Country_code STRING, Zone_name STRING""").
      csv("data/zone.csv")

    //join for combining zoneDf and airportRegionsDf
    //same output as Meet's 'exportDF'
    val allDf = zoneDf
      .join(airportRegionsDf, airportRegionsDf("Time Zone") === zoneDf("Zone_name"))
      .select(airportRegionsDf("Name").as("Airport Name"), airportRegionsDf("Time Zone"),
        airportRegionsDf("City"), zoneDf("country_code").as("Country Code"),
        split(col("Time Zone"), "/").getItem(0).as("Region"))
    // JOIN FOR extFiltered DF and zoneDf
//    val allDf = zoneDf
//      .join(extFiltered, extFiltered("Tz_database") === zoneDf("Zone_name"))
//      .select(zoneDf("country_code").as("Country Code"), extFiltered("Name").as("Airport Name"))
//      .orderBy("country_code")
    println("display allDF below")
    allDf.show(15)

    //****EXPORT #2******
    //splits into files according to country... and shows country code, airport name
    //allDf.write.mode("overwrite").option("header","true").csv("data/allAirports")

    //display abbreviated timezone with # of airports
//    val numAirPerTz = finalDF.join(all, all("zone_name") === finalDF("Time Zone")).select(all("abbreviation"), finalDF("# of Airports")).distinct()
//    numAirPerTz.show(15)
//    println("# of airports are : " + getDfCount(numAirPerTz))

    //list airport with abbreviated time zone
//    val airTz = allDf.select("Name", "Abbreviation")
//    airTz.distinct().show(50)

    reconciliation(getDfCount(extendedDf), getDfCount(zoneDf), getDfCount(extFiltered), getDfCount(regionDF), getDfCount(allDf))
    spark.stop()
  }


  //get row count of a DataFrame
  def getDfCount(df:DataFrame): Long ={
    val count = df.count()
    count
  }

  def reconciliation(extCount:Long, extFiltCount:Long, zoneCount:Long, regionCount:Long, allCount:Long){
    println("Reconciliation Strategy Details\n----------------------------------------------")
    //initial count of airports-extended.csv file
    println("The count of the initial 'airports-extended.csv' file is : " + extCount)

    // count of extFiltered dataframe
    println("The count of the initial extFiltered dataframe (aka # of airports) is : " + extFiltCount)

    //initial count of zone.csv file
    println("The count of the initial 'zone.csv' file is : " + zoneCount)

    //count of regions
    println("The number of regions where airports are found is : " + regionCount)

    //count of the join
    println("The total number of airports exported in allDf is : " + allCount)

    //call at the end of the main function
    //***********EXPORT TO TXT FILE************
    //****EXPORT #3******
    val pw = new PrintWriter(new File("data/logs/reconciliation.txt"))
    pw.write("-----Reconciliation Strategy Details-----\nThe count of the initial 'airports-extended.csv' file is : "
      + extCount
      + "\nThe count of the nitial extFiltered dataframe (aka # of airports) is : "
      + extFiltCount
      + "\nThe count of the initial 'zone.csv' file is : "
      + zoneCount
      + "\nThe number of regions where airports are found is : "
      + regionCount
      + "\nThe number of airports exported is : "
      + allCount)
    pw.close()

  }
}