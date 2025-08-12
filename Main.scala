package communityDetection

import org.graphframes._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import wcc.GraphFrameOps._
////
/////**
//// * Created by tariq on 28/11/17.
//// */
////
//object Main {
//  val FULLRUN = 1
//  val STREAM = 2
//
//  def main(args: Array[String]): Unit = {
//    // Set up Spark configuration with executor memory
//    val sparkConf = new SparkConf()
//      .setAppName("CommunityDetection")
////      .set("spark.driver.memory", "2g")
////      .set("spark.executor.memory", "3g")
//
//    // Path to the graph file (e.g., "data/com-amazon.ungraph.txt")
//    val filePath = "C:\\Users\\Kostas\\Desktop\\idwcc\\data\\orkut.txt"
//    val logFilePath = "resultsDWCC/results.txt"
//    // Check Spark configuration for master URL, set it to local if not configured
//    if (!sparkConf.contains("spark.master")) {
//      sparkConf.setMaster("local[2]")
//    }
//
//    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
//    Logger.getRootLogger.setLevel(Level.WARN)
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getRootLogger.warn("Getting context!!")
//
//    // Initialize SparkSession
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
//    Logger.getRootLogger.warn("We have context!!")
//    Logger.getRootLogger.warn(s"Dataset: $filePath")
//
//    val operation = FULLRUN
//    val queryTimeInterval: (Int, Int) = (250, 300)
//    operation match {
//      case FULLRUN => loadAndRegionalizeGraph(spark, filePath, logFilePath, queryTimeInterval)
//      case STREAM => Logger.getRootLogger.warn("STREAM operation is not supported in this context.")
//    }
//
//    // Stopping the SparkSession
//    spark.stop()
//  }
//
//  def loadAndRegionalizeGraph(spark: SparkSession, filePath: String, logFilePath: String, queryTimeInterval: (Int, Int), edgeCount: Float = 0f): Unit = {
//    // Define the time interval and overlap percentage
//    val graph = CSVGraph.loadGraph(spark, filePath, queryTimeInterval)
//    graph.cache()
//
//    Logger.getRootLogger.warn("graph is loaded!!")
//    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")
//
//    //val queryTimeIntervalLength = queryTimeInterval._2 - queryTimeInterval._1 + 1
//    val communityDF = graph.scalableCommunityDetection(spark, logFilePath)
//    val regionalizedGraph = GraphFrame(graph.vertices.join(communityDF, "id"), graph.edges).cache()
//
//    regionalizedGraph.edges.count()  // This action triggers the computation
//
//    Logger.getRootLogger.warn("Graph processing complete!!")
//  }
//}


object Main {
  val FULLRUN = 1
  val STREAM = 2

  def main(args: Array[String]): Unit = {
    // Ensure proper arguments
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
      sparkConf.setMaster("local[*]") // for local testing fallback
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
      case STREAM => Logger.getRootLogger.warn("STREAM operation is not supported in this context.")
    }

    spark.stop()
  }

  def loadAndRegionalizeGraph(spark: SparkSession, filePath: String, logFilePath: String, queryTimeInterval: (Int, Int), edgeCount: Float = 0f): Unit = {
    // Define the time interval and overlap percentage
    val graph = CSVGraph.loadGraph(spark, filePath, queryTimeInterval)
    graph.cache()

    Logger.getRootLogger.warn("graph is loaded!!")
    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    //val queryTimeIntervalLength = queryTimeInterval._2 - queryTimeInterval._1 + 1
    val communityDF = graph.scalableCommunityDetection(spark, logFilePath)
    val regionalizedGraph = GraphFrame(graph.vertices.join(communityDF, "id"), graph.edges).cache()

    regionalizedGraph.edges.count()  // This action triggers the computation

    Logger.getRootLogger.warn("Graph processing complete!!")
  }
}
