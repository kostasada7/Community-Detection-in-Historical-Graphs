package communityDetection

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.graphframes._
import java.io._

/**
 * Created by tariq on 02/05/18.
 */
object CSVGraph {
  // classic
  val dataPath = "data/cust777.txt"
  // custom path for the RDyn python script results
  // val rdynPath = "RDyn/results/1000_1000_15_0.7_0.8_0.3_1/graph-999.txt"

  def loadGraph(spark: SparkSession, dataPath: String, queryTimeInterval: (Int, Int)): GraphFrame = {
    // Define the custom schema for your data
    val customSchema = StructType(Seq(
      StructField("src", IntegerType, nullable = false),
      StructField("dst", IntegerType, nullable = false),
      StructField("attr1", IntegerType, nullable = false),
      StructField("attr2", IntegerType, nullable = false)
    ))

    // Read the CSV data with custom schema and filter out self-loops
    val edgesDF = spark.read
      .option("delimiter", "\t")
      .schema(customSchema)
      .csv(dataPath)
      .filter(row => row.getAs[Int]("src") != row.getAs[Int]("dst"))

    // Log the number of raw edges
    val logger = Logger.getLogger(getClass.getName)
    logger.warn(s"Raw edges count: ${edgesDF.count()}")

    val overlapPercentage: Float = 0f // Example overlap percentage

    // Define the UDF using the typed API
    val queryTimeIntervalLength = queryTimeInterval._2 - queryTimeInterval._1 + 1

    // Define the typed UDF
    val filterAndAdjustEdges = udf((attr1: Int, attr2: Int) => {
      val start = Math.max(attr1, queryTimeInterval._1)
      val end = Math.min(attr2, queryTimeInterval._2)
      val edgeTimeIntervalLength = end - start + 1
      val overlapPercentageActual = (edgeTimeIntervalLength / queryTimeIntervalLength) * 100.0
      if (edgeTimeIntervalLength != 0 && overlapPercentageActual >= overlapPercentage) {
        Some((start, end))
      } else {
        None
      }
    })

    // Apply the UDF to filter and adjust edges
    val adjustedEdgesDF = edgesDF
      .withColumn("newInterval", filterAndAdjustEdges(col("attr1"), col("attr2")))
      .filter(col("newInterval").isNotNull)
      .select(col("src"), col("dst"), col("newInterval._1").alias("attr1"), col("newInterval._2").alias("attr2"))

    // Log the number of filtered and adjusted edges
    logger.warn(s"Filtered and adjusted edges count: ${adjustedEdgesDF.count()}")

    // Create a GraphFrame from the DataFrame of adjusted edges
    val graphFrame = GraphFrame.fromEdges(adjustedEdgesDF)

    graphFrame
  }

  def loadSampleGraph(spark: SparkSession, dataPath: String): GraphFrame = {
    // Define the custom schema for your data
    val customSchema = StructType(Seq(
      StructField("src", IntegerType, nullable = false),
      StructField("dst", IntegerType, nullable = false),
      StructField("attr1", IntegerType, nullable = false),
      StructField("attr2", IntegerType, nullable = false)
    ))

    // Read the CSV data with custom schema and filter out self-loops
    val edgesDF = spark.read
      .option("delimiter", "\t")
      .schema(customSchema)
      .csv(dataPath)
      .filter(row => row.getAs[Int]("src") != row.getAs[Int]("dst"))

    // Log the number of raw edges
    val logger = Logger.getLogger(getClass.getName)
    logger.warn(s"Raw edges count: ${edgesDF.count()}")

    // Create a GraphFrame from the DataFrame of edges
    val graphFrame = GraphFrame.fromEdges(edgesDF)
    graphFrame
  }
}

//package communityDetection
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._
//import org.apache.log4j.Logger
//import org.graphframes.GraphFrame
//
//object CSVGraph {
//
//  def loadGraph(spark: SparkSession, dataPath: String, queryTimeInterval: (Int, Int)): GraphFrame = {
//    val customSchema = StructType(Seq(
//      StructField("src", IntegerType, nullable = false),
//      StructField("dst", IntegerType, nullable = false),
//      StructField("attr1", IntegerType, nullable = false),
//      StructField("attr2", IntegerType, nullable = false)
//    ))
//
//    val logger = Logger.getLogger(getClass.getName)
//
//    val edgesDF = spark.read
//      .option("delimiter", "\t")
//      .schema(customSchema)
//      .csv(dataPath)
//      .filter(row => row.getAs[Int]("src") != row.getAs[Int]("dst"))
//
//    logger.warn(s"Raw edges count: ${edgesDF.count()}")
//
//    //    val queryTimeIntervalLength = queryTimeInterval._2 - queryTimeInterval._1 + 1
//    //    val overlapPercentage: Float = 0f
//
//    val filterAndAdjustEdges = udf((attr1: Int, attr2: Int) => {
//      val start = Math.max(attr1, queryTimeInterval._1)
//      val end = Math.min(attr2, queryTimeInterval._2)
//      val edgeTimeIntervalLength = end - start + 1
//      if (edgeTimeIntervalLength > 0) {
//        Some((start, end))
//      } else {
//        None
//      }
//    })
//
//    val adjustedEdgesDF = edgesDF
//      .withColumn("newInterval", filterAndAdjustEdges(col("attr1"), col("attr2")))
//      .filter(col("newInterval").isNotNull)
//      .select(
//        col("src"),
//        col("dst"),
//        col("newInterval._1").alias("attr1"),
//        col("newInterval._2").alias("attr2")
//      )
//
//    logger.warn(s"Filtered and adjusted edges count: ${adjustedEdgesDF.count()}")
//
//    GraphFrame.fromEdges(adjustedEdgesDF)
//  }
//
//  //  def loadSampleGraph(spark: SparkSession, dataPath: String): GraphFrame = {
//  //    val customSchema = StructType(Seq(
//  //      StructField("src", LongType, nullable = false),
//  //      StructField("dst", LongType, nullable = false),
//  //      StructField("attr1", LongType, nullable = false),
//  //      StructField("attr2", LongType, nullable = false)
//  //    ))
//  //
//  //    val logger = Logger.getLogger(getClass.getName)
//  //
//  //    val edgesDF = spark.read
//  //      .option("delimiter", "\t")
//  //      .schema(customSchema)
//  //      .csv(dataPath)
//  //      .filter(row => row.getAs[Long]("src") != row.getAs[Long]("dst"))
//  //
//  //    logger.warn(s"Raw edges count: ${edgesDF.count()}")
//  //
//  //    GraphFrame.fromEdges(edgesDF)
//  //  }
//}