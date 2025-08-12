//package communityDetection
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
//
//import java.io._
//
//object LPA2 {
//
//  // Step 1: Filter edges by time interval and keep overlap intervals
//  private def filterEdgesByTimeIntervalWithIntervals2[VD](
//                                                           graph: Graph[VD, (Long, Long)],
//                                                           timeInterval: (Long, Long)
//                                                         ): Graph[VD, (Long, Long)] = {
//    val T_start = timeInterval._1
//    val T_end = timeInterval._2
//
//    graph.subgraph(epred = e => {
//      val (t_start, t_end) = e.attr
//      val overlapStart = math.max(t_start, T_start)
//      val overlapEnd = math.min(t_end, T_end)
//      overlapStart <= overlapEnd // Keep edges with valid overlap
//    }).mapEdges(e => {
//      val (t_start, t_end) = e.attr
//      val overlapStart = math.max(t_start, T_start)
//      val overlapEnd = math.min(t_end, T_end)
//      (overlapStart, overlapEnd) // Store overlap interval as edge attribute
//    })
//  }
//
//  private def calculateTotalIntersection2(intervals: List[(Long, Long)]): Long = {
//    if (intervals.isEmpty) {
//      0L // No intervals, return 0
//    } else {
//      // Use valid pairs of different intervals (i != j)
//      val totalIntersection = intervals.zipWithIndex.flatMap { case (interval1, i) =>
//        intervals.zipWithIndex.collect {
//          case (interval2, j) if j > i => // Only consider pairs where j > i to avoid duplicates and self-pairing
//            val overlapStart = math.max(interval1._1, interval2._1)
//            val overlapEnd = math.min(interval1._2, interval2._2)
//            val intersection = math.max(0L, overlapEnd - overlapStart + 1)
//            println(s"Combination: [(${interval1._1}, ${interval1._2}), (${interval2._1}, ${interval2._2})] -> Intersection: $intersection")
//            intersection
//        }
//      }.sum // Sum up all intersections
//
//      println(s"Total intersection: $totalIntersection")
//      totalIntersection
//    }
//  }
//
//  // Send messages with overlap intervals grouped by community ID
//  private def sendMessage2(
//                            e: EdgeTriplet[VertexId, (Long, Long)]
//                          ): Iterator[(VertexId, Map[VertexId, List[(Long, Long)]])] = {
//    Iterator(
//      (e.srcId, Map(e.dstAttr -> List(e.attr))), // Source sends overlap interval to destination's label
//      (e.dstId, Map(e.srcAttr -> List(e.attr))) // Destination sends overlap interval to source's label
//    )
//  }
//
//  // Merge messages by combining overlap intervals
//  private def mergeMessage2(
//                             msg1: Map[VertexId, List[(Long, Long)]],
//                             msg2: Map[VertexId, List[(Long, Long)]]
//                           ): Map[VertexId, List[(Long, Long)]] = {
//    (msg1.keySet ++ msg2.keySet).map { label =>
//      label -> (msg1.getOrElse(label, List()) ++ msg2.getOrElse(label, List()))
//    }.toMap
//  }
//
//  private def vertexProgram2(
//                              vid: VertexId,
//                              communityId: VertexId,
//                              message: Map[VertexId, List[(Long, Long)]]
//                            ): VertexId = {
//    if (message.isEmpty) {
//      communityId // No messages, retain current label
//    } else {
//      // Calculate total intersection for each label
//      val labelWeights = message.map { case (label, intervals) =>
//        label -> calculateTotalIntersection2(intervals)
//      }
//
//      // Choose the label with the maximum total intersection weight
//      val chosenLabel = labelWeights.maxBy(_._2)._1
//      chosenLabel
//    }
//  }
//
//
//  // Run Label Propagation with overlap intervals
//  private def runLPAWithOverlapIntervals2[VD](
//                                               graph: Graph[VD, (Long, Long)],
//                                               maxSteps: Int,
//                                               timeInterval: (Long, Long)
//                                             ): Graph[VertexId, (Long, Long)] = {
//    val filteredGraph = filterEdgesByTimeIntervalWithIntervals2(graph, timeInterval)
//
//    var currentGraph = filteredGraph.mapVertices((vid, _) => vid) // Initialize each node's community ID as its own ID
//
//    for (step <- 1 to maxSteps) {
//      println(s"--- Iteration $step: Communities ---")
//      val communities = currentGraph.vertices
//        .map { case (vertexId, communityId) => (communityId, vertexId) }
//        .groupByKey()
//      communities.collect().foreach { case (communityId, members) =>
//        println(s"Community $communityId: [${members.mkString(", ")}]")
//      }
//
//      currentGraph = Pregel(
//        currentGraph,
//        initialMsg = Map[VertexId, List[(Long, Long)]](),
//        maxIterations = 1 // Run Pregel for one iteration at a time
//      )(
//        vprog = vertexProgram2,
//        sendMsg = sendMessage2,
//        mergeMsg = mergeMessage2
//      )
//    }
//
//    currentGraph
//  }
//
//  def main(args: Array[String]): Unit = {
//    if(args.length != 3) {
//      println("Invalid number of arguments")
//      sys.exit(1)
//    }
//
//    // Edge file path
//    val edgesFilePath = args(0)
//    // Time input range
//    val startTimeInput = args(1).toLong
//    val endTimeInput = args(2).toLong
//
//    val sc = new SparkContext(new SparkConf())
//
//    // Read edges from the file
//    val edges: RDD[Edge[(Long, Long)]] = sc.textFile(edgesFilePath)
//      .filter(line => !line.startsWith("#")) // Skip header lines
//      .map { line =>
//        val fields = line.split("\\s+")
//        val srcId = fields(0).toLong
//        val dstId = fields(1).toLong
//        val start_time = fields(2).toLong
//        val end_time = fields(3).toLong
//        Edge(srcId, dstId, (start_time, end_time))
//      }
//      .filter(edge => edge.attr._1 <= endTimeInput && edge.attr._2 >= startTimeInput) // Filter by time interval
//
//    // Create graph from filtered edges RDD
//    val graph: Graph[Int, (Long, Long)] = Graph.fromEdges(edges, defaultValue = 1)
//
//    val timeInterval = (startTimeInput, endTimeInput)
//    val maxIterations = 4
//    // Start time
//    val startTime = System.nanoTime()
//    // Run Label Propagation Algorithm
//    val resultGraph = runLPAWithOverlapIntervals2(graph, maxIterations, timeInterval)
//
//    // End time after algorithm execution
//    val endTime = System.nanoTime()
//
//    // Output the results: Group vertices by community
//    val finalCommunities = resultGraph.vertices
//      .map { case (vertexId, communityId) => (communityId, vertexId) }
//      .groupByKey()
//
//    println("Detected Communities:")
//    finalCommunities.collect().foreach { case (communityId, vertexList) =>
//      println(s"Community $communityId: [${vertexList.mkString(", ")}]")
//    }
//
//    // Write the communities to the file
//    val bw = new BufferedWriter(new FileWriter("communities.txt"))
//    finalCommunities.collect().foreach { case (communityId, vertexList) =>
//      bw.write(s"Community $communityId: [${vertexList.mkString(",")}]\n")
//    }
//    bw.close()
//
//    // Calculate and display the elapsed time
//    val elapsedTime = (endTime - startTime) / 1e9 // Convert nanoseconds to seconds
//    println(f"Time taken to execute the algorithm: $elapsedTime%.3f seconds")
//
//    // Stop the Spark session
//    sc.stop()
//  }
//}

package communityDetection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import java.io._

object LPA2 {

  private def filterEdgesByTimeIntervalWithIntervals2[VD](
                                                           graph: Graph[VD, (Long, Long)],
                                                           timeInterval: (Long, Long)
                                                         ): Graph[VD, (Long, Long)] = {
    val T_start = timeInterval._1
    val T_end = timeInterval._2

    graph.subgraph(epred = e => {
      val (t_start, t_end) = e.attr
      val overlapStart = math.max(t_start, T_start)
      val overlapEnd = math.min(t_end, T_end)
      overlapStart <= overlapEnd
    }).mapEdges(e => {
      val (t_start, t_end) = e.attr
      val overlapStart = math.max(t_start, T_start)
      val overlapEnd = math.min(t_end, T_end)
      (overlapStart, overlapEnd)
    })
  }

  private def calculateTotalIntersection2(intervals: List[(Long, Long)]): Long = {
    if (intervals.isEmpty) 0L
    else {
      intervals.zipWithIndex.flatMap { case (interval1, i) =>
        intervals.zipWithIndex.collect {
          case (interval2, j) if j > i =>
            val overlapStart = math.max(interval1._1, interval2._1)
            val overlapEnd = math.min(interval1._2, interval2._2)
            math.max(0L, overlapEnd - overlapStart + 1)
        }
      }.sum
    }
  }

  private def sendMessage2(e: EdgeTriplet[VertexId, (Long, Long)]): Iterator[(VertexId, Map[VertexId, List[(Long, Long)]])] = {
    Iterator(
      (e.srcId, Map(e.dstAttr -> List(e.attr))),
      (e.dstId, Map(e.srcAttr -> List(e.attr)))
    )
  }

  private def mergeMessage2(
                             msg1: Map[VertexId, List[(Long, Long)]],
                             msg2: Map[VertexId, List[(Long, Long)]]
                           ): Map[VertexId, List[(Long, Long)]] = {
    (msg1.keySet ++ msg2.keySet).map { label =>
      label -> (msg1.getOrElse(label, List()) ++ msg2.getOrElse(label, List()))
    }.toMap
  }

  private def vertexProgram2(
                              vid: VertexId,
                              communityId: VertexId,
                              message: Map[VertexId, List[(Long, Long)]]
                            ): VertexId = {
    if (message.isEmpty) communityId
    else {
      val labelWeights = message.map { case (label, intervals) =>
        label -> calculateTotalIntersection2(intervals)
      }
      labelWeights.maxBy(_._2)._1
    }
  }

  private def runLPAWithOverlapIntervals2[VD](
                                               graph: Graph[VD, (Long, Long)],
                                               maxSteps: Int,
                                               timeInterval: (Long, Long),
                                               logWriter: BufferedWriter, startTime: Long
                                             ): Graph[VertexId, (Long, Long)] = {
    val filteredGraph = filterEdgesByTimeIntervalWithIntervals2(graph, timeInterval)
    var currentGraph = filteredGraph.mapVertices((vid, _) => vid)

    for (step <- 1 to maxSteps) {
      if (step == maxSteps) {
        val endTime = System.nanoTime()
        val elapsedTime = (endTime - startTime) / 1e9  // Convert nanoseconds to seconds

        logWriter.write(s"--- Iteration $step: Communities ---\n")

        val communities = currentGraph.vertices
          .map { case (vertexId, communityId) => (communityId, vertexId) }
          .groupByKey()

        communities.collect().foreach { case (communityId, members) =>
          logWriter.write(s"Community $communityId: [${members.mkString(", ")}]\n")
        }

        logWriter.write(f"\nTime taken to execute the algorithm: $elapsedTime%.3f seconds\n")
      }

      currentGraph = Pregel(
        currentGraph,
        initialMsg = Map[VertexId, List[(Long, Long)]](),
        maxIterations = 1
      )(
        vprog = vertexProgram2,
        sendMsg = sendMessage2,
        mergeMsg = mergeMessage2
      )
    }

    currentGraph
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: LPA2 <edgesFilePath> <startTime> <endTime> <logFilePath>")
      sys.exit(1)
    }

    val edgesFilePath = args(0)
    val logFilePath = args(1)
    val startTimeInput = args(2).toLong
    val endTimeInput = args(3).toLong

    val sc = new SparkContext(new SparkConf())

    val logWriter = new BufferedWriter(new FileWriter(logFilePath))

    val edges: RDD[Edge[(Long, Long)]] = sc.textFile(edgesFilePath)
      .filter(line => !line.startsWith("#"))
      .map { line =>
        val fields = line.split("\\s+")
        val srcId = fields(0).toLong
        val dstId = fields(1).toLong
        val start_time = fields(2).toLong
        val end_time = fields(3).toLong
        Edge(srcId, dstId, (start_time, end_time))
      }
      .filter(edge => edge.attr._1 <= endTimeInput && edge.attr._2 >= startTimeInput)

    val graph: Graph[Int, (Long, Long)] = Graph.fromEdges(edges, defaultValue = 1)
    val timeInterval = (startTimeInput, endTimeInput)
    val maxIterations = 4

    val startTime = System.nanoTime()
    val resultGraph = runLPAWithOverlapIntervals2(graph, maxIterations, timeInterval, logWriter, startTime)
//    val endTime = System.nanoTime()
//
//    val finalCommunities = resultGraph.vertices
//      .map { case (vertexId, communityId) => (communityId, vertexId) }
//      .groupByKey()
//
//    println("Detected Communities:")
//    finalCommunities.collect().foreach { case (communityId, vertexList) =>
//      println(s"Community $communityId: [${vertexList.mkString(", ")}]")
//    }
//
//    val bw = new BufferedWriter(new FileWriter("communities.txt"))
//    finalCommunities.collect().foreach { case (communityId, vertexList) =>
//      bw.write(s"Community $communityId: [${vertexList.mkString(",")}]\n")
//    }
//    bw.close()
    logWriter.close()

    sc.stop()
  }
}
