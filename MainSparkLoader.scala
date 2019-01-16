import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author sfedosov on 12/24/18.
  */
object MainSparkLoader {

  val MOST_FREQ_PROD = "most_frequent_products"
  val MOST_SPEND_COUNT = "most_spending_countries"
  val MOST_FREQ_CAT = "most_frequent_categories"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local").setJars(Array("/var/lib/hive/standalone.jar"))
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(getSchemaDefinition1)
      .load("/user/cloudera/flume/events/*/*/*")
      .cache()

    df.registerTempTable("product_purchase")

    if (isEmptyOrEqualTo(args, MOST_SPEND_COUNT)) {
      hiveContext.sql("ADD JAR /var/lib/hive/standalone.jar")
      hiveContext.sql("CREATE TEMPORARY FUNCTION getcountry AS 'GetCountryByIP'")
      val top10Countries = hiveContext.sql("SELECT getcountry(ip), SUM (price) as s FROM product_purchase GROUP BY getcountry(ip) SORT BY s desc limit 10")
      top10Countries.show()
      loadIntoDB(top10Countries, MOST_SPEND_COUNT)
    }

    if (isEmptyOrEqualTo(args, MOST_FREQ_CAT)) {
      val top10categories = sqlContext.sql("SELECT category, COUNT(*) as c FROM product_purchase GROUP BY category SORT BY c DESC LIMIT 10").toDF()
      loadIntoDB(top10categories, MOST_FREQ_CAT)
      top10categories.show()
    }

    if (isEmptyOrEqualTo(args, MOST_FREQ_PROD)) {
      val top10products = hiveContext.sql("SELECT category, product, freq FROM (" +
        "SELECT category, product, COUNT(*) AS freq, ROW_NUMBER() OVER (" +
        "PARTITION BY category ORDER BY COUNT(*) DESC) as seqnum " +
        "FROM product_purchase GROUP BY category, product) ci WHERE seqnum = 1 LIMIT 10")
      loadIntoDB(top10products, MOST_FREQ_PROD)
      top10products.show()
    }

    sc.stop()
  }

  /**
    * Ability to sql only certain queries depending on argument: you can execute either one or all queries
    */
  def isEmptyOrEqualTo(args: Array[String], str: String): Boolean = args.length == 0 || args(0).equals(str)

  private def loadIntoDB(dataFrame: DataFrame, table: String) = {
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "cloudera")
    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306/mysql", table, props)
  }

  private def getSchemaDefinition1 = {
    StructType(List(
      StructField("product", StringType, nullable = false),
      StructField("price", DoubleType, nullable = false),
      StructField("date", TimestampType, nullable = false),
      StructField("category", StringType, nullable = false),
      StructField("ip", StringType, nullable = false)))
  }
}

