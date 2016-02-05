import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DataTransformer {
  def saveCsv(df: DataFrame, fileName: String) : Unit = {
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(s"./$fileName")
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("DataTransformer").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "./data/chic.csv",
                                                             "header"->"true",
                                                             "delimiter" -> ";"))
    df.registerTempTable("chicago")
    val crimes = sqlContext.sql("select `Case Number` as caseNumber, Date, Block, `Location Description`, IUCR, Arrest, Domestic FROM chicago")
    saveCsv(crimes, "crimes.csv")

    val locations = sqlContext.sql("select Block, District FROM chicago")
    saveCsv(locations, "locations.csv")

    val districts = sqlContext.sql("select District, count(*) FROM chicago group by District")
    saveCsv(districts, "districts.csv")

    val locationDescription = sqlContext.sql("select `Location Description`, count(*) FROM chicago group by `Location Description`")
    saveCsv(locationDescription, "locationDescription.csv")

    val crimeTypes = sqlContext.sql("select FBI_Code, `Primary Type`, count(*) FROM chicago Group by FBI_Code, `Primary Type`")
    saveCsv(crimeTypes, "crimeTypes.csv")

    val localCrimeDescription = sqlContext.sql("select IUCR, Description, FBI_Code FROM chicago")
    saveCsv(localCrimeDescription, "localCrimeTypes.csv")

    sc.stop()
  }
}
// scalastyle:on println
