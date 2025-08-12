package communityDetection

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object CSVGraph {

  def loadGraph(
                 spark: SparkSession,
                 dataPath: String,
                 queryTimeInterval: (Int, Int)
               ): GraphFrame = {  // <-- Return GraphFrame, NOT Unit

    val customSchema = StructType(Seq(
      StructField("src", IntegerType, nullable = false),
      StructField("dst", IntegerType, nullable = false),
      StructField("attr1", IntegerType, nullable = false),
      StructField("attr2", IntegerType, nullable = false)
    ))

    val logger = Logger.getLogger(getClass.getName)

    val edgesDF = spark.read
      .option("delimiter", "\t")
      .schema(customSchema)
      .csv(dataPath)
      .filter(row => row.getAs[Int]("src") != row.getAs[Int]("dst"))

    logger.warn(s"Raw edges count: ${edgesDF.count()}")

    val queryStart = queryTimeInterval._1
    val queryEnd = queryTimeInterval._2
    val queryTimeIntervalLength = queryEnd - queryStart + 1

    // UDF returns Double normalized overlap or null (None) for no overlap
    val normalizedIntersectionUDF = udf { (attr1: java.lang.Integer, attr2: java.lang.Integer) =>
        val start = Math.max(attr1, queryStart)
        val end = Math.min(attr2, queryEnd)
        val len = end - start + 1
        if (start <= end) {
          java.lang.Double.valueOf(len.toDouble / queryTimeIntervalLength)
        } else null
    }




    val resultDF = edgesDF
      .withColumn("normalizedOverlap", normalizedIntersectionUDF(col("attr1"), col("attr2")))
      .filter(col("normalizedOverlap").isNotNull)
      .select(
        col("src"),
        col("dst"),
        col("normalizedOverlap")
      )

    logger.warn(s"Filtered and adjusted edges count: ${resultDF.count()}")

    // Create GraphFrame from filtered edges and return it
    GraphFrame.fromEdges(resultDF)
  }
}


//package communityDetection
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.types._
//import org.apache.log4j.Logger
//import org.apache.spark.sql.functions._
//import org.graphframes._
//
//object CSVGraph {
//
//  /**
//   * Load a graph from a tab-separated file with columns: src, dst, attr1 (weight).
//   * Filters out self-loops and builds a GraphFrame.
//   */
//  def loadGraph(spark: SparkSession, dataPath: String): GraphFrame = {
//    val customSchema = StructType(Seq(
//      StructField("src", LongType, nullable = false),
//      StructField("dst", LongType, nullable = false),
//      StructField("attr1", DoubleType, nullable = false)
//    ))
//
//    val edgesDF = spark.read
//      .option("delimiter", "\t")
//      .schema(customSchema)
//      .csv(dataPath)
//      .filter(row => row.getAs[Long]("src") != row.getAs[Long]("dst"))
//
//    val logger = Logger.getLogger(getClass.getName)
//    logger.warn(s"Raw edges count: ${edgesDF.count()}")
//
//    val adjustedEdgesDF = edgesDF.select("src", "dst", "attr1")
//
//    GraphFrame.fromEdges(adjustedEdgesDF)
//  }
//
//  /**
//   * Load a sample graph with additional attributes (attr1, attr2).
//   * Filters out self-loops and builds a GraphFrame.
//   */
//  def loadSampleGraph(spark: SparkSession, dataPath: String): GraphFrame = {
//    val customSchema = StructType(Seq(
//      StructField("src", LongType, nullable = false),
//      StructField("dst", LongType, nullable = false),
//      StructField("attr1", LongType, nullable = false),
//      StructField("attr2", LongType, nullable = false)
//    ))
//
//    val edgesDF = spark.read
//      .option("delimiter", "\t")
//      .schema(customSchema)
//      .csv(dataPath)
//      .filter(row => row.getAs[Long]("src") != row.getAs[Long]("dst"))
//
//    val logger = Logger.getLogger(getClass.getName)
//    logger.warn(s"Raw edges count: ${edgesDF.count()}")
//
//    GraphFrame.fromEdges(edgesDF)
//  }
//}

