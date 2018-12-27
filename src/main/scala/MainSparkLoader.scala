import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author sfedosov on 12/24/18.
  */
object MainSparkLoader extends App {

  val conf = new SparkConf().setAppName("MyApp").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .schema(getSchemaDefinition1)
    .load("/user/cloudera/flume/events/*/*/*")
    .cache()

  df.registerTempTable("product_purchase")

  /*val ipToGeo = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .schema(getSchemaDefinitionIpGeo)
    .load("/user/cloudera/spark/iptogeoname.csv")
    .toDF()

  ipToGeo.registerTempTable("ipToGeo")

  val geoToName = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .schema(getSchemaDefinitionGeoToName)
    .load("/user/cloudera/spark/geonametocountry.csv")
    .toDF()

  geoToName.registerTempTable("geoToName")

  val joined = ipToGeo.join(geoToName)

  joined.select("ip", "name").show()*/

  val top10categories = sqlContext.sql("SELECT category, COUNT(*) as c FROM product_purchase GROUP BY category SORT BY c DESC LIMIT 10").toDF()
//  val columns = Seq[String]("category", "product", functions.count("*").as("freq"),
//    functions.row_number().over(Window.partitionBy("category").orderBy(functions.count("*").desc.as("seqnum"))))
//  val colNames = columns.map(name => col(name))
//  df.select(colNames:_*).
  val top10products = sqlContext.sql("SELECT category, product, freq FROM " +
    "(SELECT category, product, COUNT(*) AS freq, " +
    "ROW_NUMBER() OVER PARTITION BY category ORDER BY COUNT(*) DESC as seqnum " +
    "FROM product_purchase GROUP BY category, product) ci WHERE seqnum = 1 LIMIT 10;").toDF()

  loadIntoDB(top10categories, "most_frequent_categories")
  loadIntoDB(top10products, "most_frequent_products")

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

  /*private def getSchemaDefinitionIpGeo = {
    StructType(List(
      StructField("ip", StringType, nullable = false),
      StructField("geo", IntegerType, nullable = false),
      StructField("smth", StringType, nullable = true),
      StructField("smth1", StringType, nullable = true),
      StructField("smth2", StringType, nullable = true),
      StructField("smth3", StringType, nullable = true)))
  }

  private def getSchemaDefinitionGeoToName = {
    StructType(List(
      StructField("geo", IntegerType, nullable = false),
      StructField("code", StringType, nullable = false),
      StructField("code1", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("code2", StringType, nullable = false),
      StructField("code3", StringType, nullable = false),
      StructField("code4", StringType, nullable = false)))
  }*/

}
