package communityDetection

import org.graphframes._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import wcc.GraphFrameOps._

object Main_Int {
  val FULLRUN = 1

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: Main <inputFilePath> <outputFilePath> <startTime> <endTime>")
      System.exit(1)
    }

    val filePath = args(0)
    val logFilePath = args(1)
    val startTime = args(2).toInt
    val endTime = args(3).toInt
    val queryTimeInterval: (Int, Int) = (startTime, endTime)

    val sparkConf = new SparkConf()
      .setAppName("CommunityDetection")

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    Logger.getRootLogger.warn("We have context!!")
    Logger.getRootLogger.warn(s"Dataset: $filePath")

    val operation = FULLRUN
    operation match {
      case FULLRUN => loadAndRegionalizeGraph(spark, filePath, logFilePath, queryTimeInterval)
    }

    spark.stop()
  }

  def loadAndRegionalizeGraph(spark: SparkSession, filePath: String, logFilePath: String, queryTimeInterval: (Int, Int), edgeCount: Int = 0): Unit = {
    val graph = CSVGraph.loadGraph(spark, filePath, queryTimeInterval)
    graph.cache()

    Logger.getRootLogger.warn("graph is loaded!!")
    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    val queryTimeIntervalLength = queryTimeInterval._2 - queryTimeInterval._1 + 1
    val communityDF = graph.scalableCommunityDetection(spark, logFilePath, queryTimeInterval, queryTimeIntervalLength)

    val regionalizedGraph = GraphFrame(graph.vertices.join(communityDF, "id"), graph.edges).cache()
    regionalizedGraph.edges.count()

    Logger.getRootLogger.warn("Graph processing complete!!")
  }
}

