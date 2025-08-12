package communityDetection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import java.io._

object LPA {

  // Step 1: Filter edges by time interval and calculate weights based on overlap duration
  private def filterEdgesByTimeIntervalWithWeights[VD](graph: Graph[VD, (Long, Long)], timeInterval: (Long, Long)): Graph[VD, Double] = {
    val T_start = timeInterval._1
    val T_end = timeInterval._2

    val filteredGraph = graph.subgraph(epred = e => {
      val (t_start, t_end) = e.attr
      val overlapStart = math.max(t_start, T_start)
      val overlapEnd = math.min(t_end, T_end)
      val overlapDuration = math.max(0L, overlapEnd - overlapStart + 1)
      overlapDuration > 0
    }).mapEdges(e => {
      val (t_start, t_end) = e.attr
      val overlapStart = math.max(t_start, T_start)
      val overlapEnd = math.min(t_end, T_end)
      val overlapDuration = math.max(0L, overlapEnd - overlapStart).toDouble
      overlapDuration
    })

    filteredGraph
  }

  // Send messages with weights grouped by community ID
  def sendMessage(e: EdgeTriplet[VertexId, Double]): Iterator[(VertexId, Map[VertexId, Double])] = {
    Iterator(
      (e.srcId, Map(e.dstAttr -> e.attr)), // Source sends weight to destination's community label
      (e.dstId, Map(e.srcAttr -> e.attr))  // Destination sends weight to source's community label
    )
  }

  // Merge the weights from different communities
  def mergeMessage(count1: Map[VertexId, Double], count2: Map[VertexId, Double]): Map[VertexId, Double] = {
    (count1.keySet ++ count2.keySet).map { label =>
      label -> (count1.getOrElse(label, 0.0) + count2.getOrElse(label, 0.0))
    }.toMap
  }

  // Vertex Program: Select the label with the maximum aggregated weight
  def vertexProgram(vid: VertexId, communityId: VertexId, message: Map[VertexId, Double]): VertexId = {
    if (message.isEmpty) {
      communityId
    } else {
      println(s"Node $vid received messages from neighbors with the following weights:")
      message.foreach { case (label, weight) =>
        println(f"  From community $label, node $vid receives total weight = $weight%.2f")
      }
      val chosenLabel = message.maxBy(_._2)._1
      println(f"Node $vid is choosing community $chosenLabel with total weight ${message(chosenLabel)}%.2f")
      chosenLabel
    }
  }

  // Run Label Propagation with time-weighted edges
  private def runLPAWithTimeWeightedEdges[VD](graph: Graph[VD, (Long, Long)], maxSteps: Int, timeInterval: (Long, Long)): Graph[VertexId, Double] = {
    val filteredGraph = filterEdgesByTimeIntervalWithWeights(graph, timeInterval)

    var currentGraph = filteredGraph.mapVertices((vid, _) => vid) // Initialize each node's community ID as its own ID

    for (step <- 1 to maxSteps) {
      println(s"--- Iteration $step: Communities ---")
      val communities = currentGraph.vertices
        .map { case (vertexId, communityId) => (communityId, vertexId) }
        .groupByKey()
      communities.collect().foreach { case (communityId, members) =>
        println(s"Community $communityId: [${members.mkString(", ")}]")
      }

      currentGraph = Pregel(
        currentGraph,
        initialMsg = Map[VertexId, Double](),
        maxIterations = 1 // Run Pregel for one iteration at a time
      )(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
      )
    }

    currentGraph
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      println("Invalid number of arguments")
      sys.exit(1)
    }

    // Edge file path
    val edgesFilePath = args(0)
    // Time input range
    val startTimeInput = args(1).toLong
    val endTimeInput = args(2).toLong

    val sc = new SparkContext(new SparkConf())

    // Read edges from the file
    val edges: RDD[Edge[(Long, Long)]] = sc.textFile(edgesFilePath)
      .filter(line => !line.startsWith("#")) // Skip header lines
      .map { line =>
        val fields = line.split("\\s+")
        val srcId = fields(0).toLong
        val dstId = fields(1).toLong
        val start_time = fields(2).toLong
        val end_time = fields(3).toLong
        Edge(srcId, dstId, (start_time, end_time))
      }
      .filter(edge => edge.attr._1 <= endTimeInput && edge.attr._2 >= startTimeInput) // Filter by time interval

    // Create graph from filtered edges RDD
    val graph: Graph[Int, (Long, Long)] = Graph.fromEdges(edges, defaultValue = 1)

    val timeInterval = (startTimeInput, endTimeInput)
    val maxIterations = 4
    // Start time
    val startTime = System.nanoTime()
    // Run Label Propagation Algorithm
    val resultGraph = runLPAWithTimeWeightedEdges(graph, maxIterations, timeInterval)
    // End time after algorithm execution
    val endTime = System.nanoTime()
    // Output the results: Group vertices by community
    val finalCommunities = resultGraph.vertices
      .map { case (vertexId, communityId) => (communityId, vertexId) }
      .groupByKey()

    println("Detected Communities:")
    finalCommunities.collect().foreach { case (communityId, vertexList) =>
      println(s"Community $communityId: [${vertexList.mkString(", ")}]")
    }

    // Write the communities to the file
    val bw = new BufferedWriter(new FileWriter("communities.txt"))
    finalCommunities.collect().foreach { case (communityId, vertexList) =>
      bw.write(s"Community $communityId: [${vertexList.mkString(",")}]\n")
    }
    bw.close()
    // Calculate and display the elapsed time
    val elapsedTime = (endTime - startTime) / 1e9 // Convert nanoseconds to seconds
    println(f"Time taken to execute the algorithm: $elapsedTime%.3f seconds")
    // Stop the Spark session
    sc.stop()
  }
}

//package communityDetection
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
//
//import java.io._
//
//object LPA {
//
//  // Step 1: Filter edges by time interval and calculate weights based on overlap duration
//  private def filterEdgesByTimeIntervalWithWeights[VD](graph: Graph[VD, (Long, Long)], timeInterval: (Long, Long)): Graph[VD, Double] = {
//    val T_start = timeInterval._1
//    val T_end = timeInterval._2
//
//    val filteredGraph = graph.subgraph(epred = e => {
//      val (t_start, t_end) = e.attr
//      val overlapStart = math.max(t_start, T_start)
//      val overlapEnd = math.min(t_end, T_end)
//      val overlapDuration = math.max(0L, overlapEnd - overlapStart + 1)
//      overlapDuration > 0
//    }).mapEdges(e => {
//      val (t_start, t_end) = e.attr
//      val overlapStart = math.max(t_start, T_start)
//      val overlapEnd = math.min(t_end, T_end)
//      val overlapDuration = math.max(0L, overlapEnd - overlapStart).toDouble
//      overlapDuration
//    })
//
//    filteredGraph
//  }
//
//  // Send messages with weights grouped by community ID
//  def sendMessage(e: EdgeTriplet[VertexId, Double]): Iterator[(VertexId, Map[VertexId, Double])] = {
//    Iterator(
//      (e.srcId, Map(e.dstAttr -> e.attr)), // Source sends weight to destination's community label
//      (e.dstId, Map(e.srcAttr -> e.attr))  // Destination sends weight to source's community label
//    )
//  }
//
//  // Merge the weights from different communities
//  def mergeMessage(count1: Map[VertexId, Double], count2: Map[VertexId, Double]): Map[VertexId, Double] = {
//    (count1.keySet ++ count2.keySet).map { label =>
//      label -> (count1.getOrElse(label, 0.0) + count2.getOrElse(label, 0.0))
//    }.toMap
//  }
//
//  // Vertex Program: Select the label with the maximum aggregated weight
//  def vertexProgram(vid: VertexId, communityId: VertexId, message: Map[VertexId, Double]): VertexId = {
//    if (message.isEmpty) {
//      communityId
//    } else {
//      message.maxBy(_._2)._1
//    }
//  }
//
//  // Run Label Propagation with time-weighted edges
//  private def runLPAWithTimeWeightedEdges[VD](
//                                               graph: Graph[VD, (Long, Long)],
//                                               maxSteps: Int,
//                                               timeInterval: (Long, Long),
//                                               logWriter: BufferedWriter, startTime:Long
//                                             ): Graph[VertexId, Double] = {
//
//    val filteredGraph = filterEdgesByTimeIntervalWithWeights(graph, timeInterval)
//    var currentGraph = filteredGraph.mapVertices((vid, _) => vid)
//
//    for (step <- 1 to maxSteps) {
//      if (step == maxSteps) {
//        val endTime = System.nanoTime()
//        val elapsedTime = (endTime - startTime) / 1e9  // Convert nanoseconds to seconds
//
//        logWriter.write(s"--- Iteration $step: Communities ---\n")
//
//        val communities = currentGraph.vertices
//          .map { case (vertexId, communityId) => (communityId, vertexId) }
//          .groupByKey()
//
//        communities.collect().foreach { case (communityId, members) =>
//          logWriter.write(s"Community $communityId: [${members.mkString(", ")}]\n")
//        }
//
//        logWriter.write(f"\nTime taken to execute the algorithm: $elapsedTime%.3f seconds\n")
//      }
//
//      currentGraph = Pregel(
//        currentGraph,
//        initialMsg = Map[VertexId, Double](),
//        maxIterations = 1
//      )(
//        vprog = vertexProgram,
//        sendMsg = sendMessage,
//        mergeMsg = mergeMessage
//      )
//    }
//    currentGraph
//  }
//
//  def main(args: Array[String]): Unit = {
//    if (args.length != 4) {
//      println("Usage: LPA <edgesFilePath> <startTime> <endTime> <logFilePath>")
//      sys.exit(1)
//    }
//
//    val edgesFilePath = args(0)
//    val logFilePath = args(1)
//    val startTimeInput = args(2).toLong
//    val endTimeInput = args(3).toLong
//
//    val sc = new SparkContext(new SparkConf())
//
//    val logWriter = new BufferedWriter(new FileWriter(logFilePath))
//
//    val edges: RDD[Edge[(Long, Long)]] = sc.textFile(edgesFilePath)
//      .filter(line => !line.startsWith("#"))
//      .map { line =>
//        val fields = line.split("\\s+")
//        val srcId = fields(0).toLong
//        val dstId = fields(1).toLong
//        val start_time = fields(2).toLong
//        val end_time = fields(3).toLong
//        Edge(srcId, dstId, (start_time, end_time))
//      }
//      .filter(edge => edge.attr._1 <= endTimeInput && edge.attr._2 >= startTimeInput)
//
//    val graph: Graph[Int, (Long, Long)] = Graph.fromEdges(edges, defaultValue = 1)
//
//    val timeInterval = (startTimeInput, endTimeInput)
//    val maxIterations = 4
//
//    val startTime = System.nanoTime()
//    val resultGraph = runLPAWithTimeWeightedEdges(graph, maxIterations, timeInterval, logWriter, startTime)
//
//
////    val finalCommunities = resultGraph.vertices
////      .map { case (vertexId, communityId) => (communityId, vertexId) }
////      .groupByKey()
//
////    println("Detected Communities:")
////    finalCommunities.collect().foreach { case (communityId, vertexList) =>
////      println(s"Community $communityId: [${vertexList.mkString(", ")}]")
////    }
//
////    val bw = new BufferedWriter(new FileWriter("communities.txt"))
////
////    // Write final communities
////    finalCommunities.collect().foreach { case (communityId, vertexList) =>
////      bw.write(s"Community $communityId: [${vertexList.mkString(",")}]\n")
////    }
////
////    // Measure and write elapsed time
////    val elapsedTime = (endTime - startTime) / 1e9  // Convert nanoseconds to seconds
////    bw.write(f"Time taken to execute the algorithm: $elapsedTime%.3f seconds\n")
////
////    // Close resources
////    bw.close()
//    logWriter.close()
//
//
//    sc.stop()
//  }
//}



