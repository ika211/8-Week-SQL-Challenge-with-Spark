package Solutions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

import java.util.Properties

object caseStudy1_DannysDiner extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Temp")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val uri = "jdbc:postgresql://localhost:5432/postgres"
  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.put("user", "postgres")
  connectionProperties.put("password", "password")

  var query = "dannys_diner.menu"

  val menu = spark.read.jdbc(uri, query, connectionProperties)

  //  val menu = spark.read
  //    .format("jdbc")
  //    .option("url", "jdbc:postgresql://localhost:5432/postgres")
  //    .option("dbtable", "dannys_diner.menu")
  //    .option("user", "postgres")
  //    .option("password", "password")
  //    .load()

  query = "dannys_diner.members"
  val members = spark.read.jdbc(uri, query, connectionProperties)

  query = "dannys_diner.sales"
  val sales = spark.read.jdbc(uri, query, connectionProperties)

  //  val aggregatedSales = sales
  //    .groupBy("customer_id", "product_id")
  //    .agg(count("*").as("quantity"))
  //
  //  val aggregatedSalesWithPrices = aggregatedSales
  //    .join(menu,usingColumn = "product_id")
  //    .select("customer_id", "product_id", "quantity", "price")
  //    .withColumn("item_total", col("quantity") * col("price"))
  //
  //  val amountPerCustomer = aggregatedSalesWithPrices
  //    .groupBy("customer_id")
  //    .agg(sum("item_total").as("total_spent"))

  val amountPerCustomer = sales
    .join(menu, usingColumn = "product_id")
    .groupBy("customer_id")
    .agg(sum("price").alias("total_spent"))

  //  amountPerCustomer.show()

  val daysVisitedPerCustomer = sales
    .groupBy("customer_id")
    .agg(countDistinct("order_date").alias("no-of-days-visited"))

  //  daysVisitedPerCustomer.show()
  val window = Window
    .partitionBy("customer_id")
    .orderBy("order_date")

  val firstItemPerCustomer = sales
    .withColumn("row_number", row_number() over window)
    .filter(col("row_number") === 1)
    .join(broadcast(menu), usingColumn = "product_id")
    .select("customer_id", "product_name")

  //  firstItemPerCustomer.show()

  val mostPurchasedItem = sales
    .groupBy("product_id")
    .agg(count("*").alias("times_ordered"))
    .orderBy(col("times_ordered").desc)
    .limit(1)
    .join(menu, usingColumn = "product_id")
    .select("product_id", "product_name", "times_ordered")

  //  mostPurchasedItem.show()


  val ByCustId: WindowSpec = Window
    .partitionBy("customer_id")
    .orderBy(col("times_ordered").desc)

  val mostPopularPerCustomer = sales
    .groupBy("customer_id", "product_id")
    .agg(count("*").as("times_ordered"))
    .withColumn("r", row_number() over ByCustId)
    .filter(col("r") === 1)
    .select("customer_id", "product_id", "times_ordered")

  //  mostPopularPerCustomer.show()


  val firstPurchasedByMember = sales
    .join(broadcast(members), usingColumn = "customer_id")
    .filter(col("order_date") >= col("join_date"))
    .withColumn("row_number", row_number() over window)
    .filter(col("row_number") === 1)
    .join(broadcast(menu), usingColumn = "product_id")
    .select("customer_id", "product_name", "order_date", "join_date")

  //  firstPurchasedByMember.show()


  val LastPurchaseBeforeMember = sales
    .join(broadcast(members), usingColumn = "customer_id")
    .filter(col("order_date") < col("join_date"))
    .withColumn("rank", rank() over Window.partitionBy("customer_id").orderBy(col("order_date").desc))
    .filter(col("rank") === 1)
    .join(broadcast(menu), usingColumn = "product_id")
    .select("customer_id", "product_name", "order_date", "join_date")

  //  LastPurchaseBeforeMember.show()


  // non members include
  val totalSalesBeforeMember1 = sales
    .join(broadcast(members), sales("customer_id") === members("customer_id"), "left")
    .join(broadcast(menu), sales("product_id") === menu("product_id"), joinType = "inner")
    .filter(col("join_date").isNull ||
      (col("join_date").isNotNull
        && col("order_date") < col("join_date")))
    .groupBy(sales("customer_id"))
    .agg(count("*").as("total_items"),
      sum("price").as("amount_spent"))

  //  totalSalesBeforeMember1.show()


  //    non members not included
  val totalSalesBeforeMember = sales
    .join(broadcast(members), sales("customer_id") === members("customer_id"))
    .join(broadcast(menu), sales("product_id") === menu("product_id"))
    .filter(col("order_date") < col("join_date"))
    .groupBy(sales("customer_id"))
    .agg(count("*").as("total_items"),
      sum("price").as("amount_spent"))

  //    totalSalesBeforeMember.show()


  val pointsPerCustomer = sales
    .join(broadcast(menu), usingColumn = "product_id")
    .withColumn("points",
      col("price") * when(col("product_name") === "sushi", 20).otherwise(10))
    .groupBy("customer_id")
    .agg(sum("points").alias("total_points"))

  //  pointsPerCustomer.show()


  val pointsPerCustomerFirstWeekMembership = sales
    .join(broadcast(members), sales("customer_id") === members("customer_id"))
    .join(broadcast(menu), usingColumn = "product_id")
    .filter(col("order_date") <= "2021-01-31")
    .withColumn("datediff", datediff(col("order_date"), col("join_date")))
    .withColumn("points",
      col("price") * when(col("datediff") >= 0 && col("datediff") <= 7, 20)
        .otherwise(when(col("product_name") === "sushi", 20).otherwise(10)))
    .groupBy(sales("customer_id"))
    .agg(sum("points").alias("total_points"))

  pointsPerCustomerFirstWeekMembership.show()
}
