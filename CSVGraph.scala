package communityDetection

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.graphframes.GraphFrame

object CSVGraph {

  def loadGraph(spark: SparkSession, dataPath: String, queryTimeInterval: (Int, Int)): GraphFrame = {
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

    val filterAndAdjustEdges = udf((attr1: Int, attr2: Int) => {
      val start = Math.max(attr1, queryTimeInterval._1)
      val end = Math.min(attr2, queryTimeInterval._2)
      val edgeTimeIntervalLength = end - start + 1
      if (edgeTimeIntervalLength > 0) {
        Some((start, end))
      } else {
        None
      }
    })

    val adjustedEdgesDF = edgesDF
      .withColumn("newInterval", filterAndAdjustEdges(col("attr1"), col("attr2")))
      .filter(col("newInterval").isNotNull)
      .select(
        col("src"),
        col("dst"),
        col("newInterval._1").alias("attr1"),
        col("newInterval._2").alias("attr2")
      )

    logger.warn(s"Filtered and adjusted edges count: ${adjustedEdgesDF.count()}")

    GraphFrame.fromEdges(adjustedEdgesDF)
  }
}
