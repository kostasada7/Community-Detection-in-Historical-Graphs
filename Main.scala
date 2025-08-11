package communityDetection

import org.graphframes._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import wcc.GraphFrameOps._

/**
 * Created by tariq on 28/11/17.
 */

object Main {
  val FULLRUN = 1
  val STREAM = 2

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CommunityDetection")

    // Path to the graph file (e.g"RDyn/results/1000_1000_15_0.7_0.8_0.3_1/graph-999.txt"., "data/com-amazon.ungraph.txt")
   val filePath = "hdfs://150.140.142.45:9000/idwcc_data/com-dblp.ungraph.txt"
//   val filePath = "/home/vagg/Desktop/Datasets/com-dblp.ungraph.txt"

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }


    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)

    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")

    // Load and partition Graph
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    Logger.getRootLogger.warn("We have context!!")
    Logger.getRootLogger.warn(s"Dataset: $filePath")

    val operation = FULLRUN

    operation match {

      case FULLRUN =>
        loadAndRegionalizeGraph(spark, filePath)

      case STREAM =>
        CSVGraph.testStream(spark, filePath)

    }

    ssc.start()
    ssc.awaitTermination()

  }

  def loadAndRegionalizeGraph(spark: SparkSession, filePath: String, edgeCount:Double=0) = {
    val graph = if (edgeCount == 0) {
      CSVGraph.loadGraph(spark, filePath)
    } else {
      CSVGraph.loadSampleGraph(spark, filePath, edgeCount)
    }
    graph.cache()

    Logger.getRootLogger.warn("graph is loaded!!")
    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    // JOIN GRAPH WITH COMMUNITIES
    val communityDF = graph.scalableCommunityDetection(spark)
    val regionalizedGraph = GraphFrame(graph.vertices.join(communityDF, "id"), graph.edges).cache()
    regionalizedGraph
  }
}