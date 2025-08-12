package wcc

import communityDetection.TriangleScoringWithTimeIntervals

import java.io.{File, IOException, PrintWriter}
import org.apache.log4j.Logger

import scala.reflect.ClassTag
import scala.collection.mutable
import scalaz.Scalaz._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{VertexRDD, _}
import wcc.GraphXOps.GXOperations

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by tariq on 31/12/17.
 * Implementation of the "Scalable Community Detection" algorithm.
 * @note We always cache a new graph if it will be used multiple time (and un-cache
 * it afterwards).
 * @see "High quality, scalable and parallel community detection for large real graphs"
 *      https://dl.acm.org/citation.cfm?id=2568010
 * @see "Distributed Community Detection with the WCC Metric"
 *      https://dl.acm.org/citation.cfm?id=2744715
 * @see "Shaping communities out of triangles"
 *      https://dl.acm.org/citation.cfm?id=2398496
 */
object DistributedWCC {

  // Main parameters
  private val threshold = 0.01f // default threshold 0.01
  private var maxRetries = 5 // default number of retries 5
  private var numPartitions = 200 // default number of partitions 200
  var vertexCount = 0L
  // Main parameters end
  // --------------------------------------------

  def runWithStats[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext, logFilePath: String,
                                               maxRetries: Int = this.maxRetries,
                                               partitions: Int = this.numPartitions,
                                               isCanonical: Boolean = false, queryTimeIntervalLength: Long
                                              ): (Graph[VertexData, ED], Map[VertexId, CommunityData]) = {
    require(maxRetries > 0, s"Number of iterations must be greater than or equal to 0," +
      s" but got $maxRetries")
    require(partitions > 0, s"Number of partitions must be greater than 0," +
      s" but got $partitions")

    this.maxRetries = maxRetries
    this.numPartitions = partitions
    this.vertexCount = graph.vertices.count

    val startTime = System.nanoTime()

    val optimizedGraph = preprocess(graph, isCanonical)
    val initGraph = performInitialPartition(optimizedGraph)
    val (communityGraph, cStats) = refinePartition(initGraph, sc)

    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9  // Convert nanoseconds to seconds

    val dataGraph = graph.outerJoinVertices(communityGraph.vertices)((vId, _, vDataOpt) =>
      vDataOpt.getOrElse(new VertexData(vId, 0, 0, 0f))
    )

    (dataGraph, cStats)
  }

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext, logFilePath: String,
                                      maxRetries: Int = this.maxRetries,
                                      partitions: Int = this.numPartitions,
                                      isCanonical: Boolean = false
                                     ): VertexRDD[VertexId] = {
    require(maxRetries >= 0, s"Number of iterations must be greater than or equal to 0," +
      s" but got $maxRetries")
    require(partitions > 0, s"Number of partitions must be greater than 0," +
      s" but got $partitions")

    this.maxRetries = maxRetries
    this.numPartitions = partitions
    this.vertexCount = graph.vertices.count

    val startTime = System.nanoTime()

    Logger.getRootLogger.warn("Phase: Preprocessing Start...")
    val optimizedGraph = preprocess(graph, isCanonical)

    Logger.getRootLogger.warn("Phase: Community Initialization Start...")
    val initGraph = performInitialPartition(optimizedGraph)
    //val initCommunityMap = initGraph.vertices.mapValues((vId, vData) => vData.cId)
//    printStats(initCommunityMap)
//
//    Logger.getRootLogger.warn("Phase: WCC Iteration Start...")
    val communityMap = refinePartition(initGraph, sc)._1.vertices.mapValues((vId, vData) => vData.cId)
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9  // Convert nanoseconds to seconds
    //val communityMap = refinedGraph.vertices.mapValues((vId, vData) => vData.cId)

    //    // Compute community statistics AFTER refinement
//    val communityStats = computeCommunityStats(refinedGraph)
//
//    Logger.getRootLogger.warn("Computed Community Statistics:")
//    communityStats.foreach { case (community, data) =>
//      Logger.getRootLogger.warn(s"Community $community -> Size: ${data.r}, " +
//        s"Internal Edges: ${data.a}, External Edges: ${data.b}, Avg CC: ${data.avgCC}")
//    }
//    printStats(communityMap)
    printCommunities(communityMap,logFilePath,elapsedTime) // print the analytical communities with the vertices inside
    // store the analytical communities with the vertices inside
    //countTriangles(graph).collect().foreach(println)      // final triangles

    communityMap
  }

  def printStats(communityMap: VertexRDD[VertexId]): Unit = {
    Logger.getRootLogger.warn(s"Generated ${communityMap.values.distinct.count()} communities.")
    val majorCommunityStats = communityMap.map(x => (x._2, 1L)).reduceByKey(_ + _).filter(_._2 > 2)
    Logger.getRootLogger.warn(s"Generated ${majorCommunityStats.count()} major communities.")
    majorCommunityStats.sortBy(_._2, ascending = false).take(10).foreach(println)
    // main parameters used
    Logger.getRootLogger.warn(s"Threshold: $threshold")
    Logger.getRootLogger.warn(s"Max Retries: $maxRetries")
    Logger.getRootLogger.warn(s"Number of Partitions: $numPartitions")
    Logger.getRootLogger.warn(s"Number of Vertices: $vertexCount")

  }

  def printCommunities(communityMap: VertexRDD[VertexId], logFilePath: String, elapsedTime:Double): Int = {
    // Group vertices by community ID (communityId -> Iterable[vertexId])
    val communities = communityMap
      .map { case (vertexId, communityId) => (communityId, vertexId) }
      .groupByKey()
      .sortByKey(ascending = true)

    // Count how many distinct communities
    val distinctCommunitiesCount = communities.count().toInt

    // Format communities for output as strings
    val formattedCommunities = communities.map { case (cId, vertices) =>
      s"Community $cId: ${vertices.mkString(", ")}"
    }.collect().toSeq  // collect to driver for logging to single file

    // Call updated logCommunities that accepts formatted lines
    logCommunities(formattedCommunities, logFilePath, elapsedTime)

    // Log count
    Logger.getRootLogger.warn(s"Generated $distinctCommunitiesCount distinct communities.")
    println(s"Communities have been saved to $logFilePath")

    distinctCommunitiesCount
  }

  def logCommunities(formattedCommunities: Seq[String], logFilePath: String, elapsedTime: Double): Unit = {
    val path = Paths.get(logFilePath)

    // Create the file only if it doesn't exist
    if (!Files.exists(path)) {
      Files.createFile(path)
    }

    val writer = new PrintWriter(logFilePath)
    try {
      // Write each formatted community
      formattedCommunities.foreach(writer.println)

      // Write elapsed time at the end
      writer.println(f"\nTime taken to execute the algorithm: $elapsedTime%.3f seconds")
    } finally {
      writer.close()
    }

    val distinctCount = formattedCommunities.size
    Logger.getRootLogger.warn(s"Generated $distinctCount communities.")
    println(s"Communities have been logged to $logFilePath")
  }


  //  def printStats(communityMap: VertexRDD[VertexId]): Unit = {
//    Logger.getRootLogger.warn(s"Generated ${communityMap.values.distinct.count()} communities.")
//    val majorCommunityStats = communityMap.map(x => (x._2, 1L)).reduceByKey(_ + _).filter(_._2 > 2)
//    Logger.getRootLogger.warn(s"Generated ${majorCommunityStats.count()} major communities.")
//    majorCommunityStats.sortBy(_._2, ascending = false).take(10).foreach(println)
//    // main parameters used
//    Logger.getRootLogger.warn(s"Threshold: $threshold")
//    Logger.getRootLogger.warn(s"Max Retries: $maxRetries")
//    Logger.getRootLogger.warn(s"Number of Partitions: $numPartitions")
//    Logger.getRootLogger.warn(s"Number of Vertices: $vertexCount")
//
//  }
//
//  def printCommunities(communityMap: VertexRDD[VertexId]): Int = {
//    // Group vertices by community ID
//    val communities = communityMap.map(x => (x._2, x._1)).groupByKey().sortByKey(ascending = true)
//
//    // To store the final communities after potential merging
//    var finalCommunities = Map[VertexId, Set[VertexId]]()
//
//    // Collect the communities to the driver for processing
//    val collectedCommunities = communities.collect()
//
//    // Function to find the community containing a hub node
//    def findContainingCommunity(hubId: VertexId): Option[VertexId] = {
//      collectedCommunities.find { case (cId, vertices) =>
//        cId != hubId && vertices.toSet.contains(hubId)
//      }.map(_._1)
//    }
//
//    // Iterate through each community
//    collectedCommunities.foreach { case (cId, vertices) =>
//      // Convert Iterable to Set to use 'contains' method
//      val vertexSet = vertices.toSet
//
//      // Check if the community hub (cId) exists in its own community
//      val hubInItsCommunity = vertexSet.contains(cId)
//
//      if (!hubInItsCommunity) {
//        // Find another community that contains the hub
//        val otherCIdOption = findContainingCommunity(cId)
//
//        // Transfer vertices to the community containing the hub if found
//        otherCIdOption.foreach { otherCId =>
//          val mergedVertices = finalCommunities.getOrElse(cId, Set()) ++
//            finalCommunities.getOrElse(otherCId, Set()) ++
//            vertexSet ++
//            collectedCommunities.find(_._1 == otherCId).map(_._2).getOrElse(Set())
//
//          finalCommunities += (cId -> mergedVertices)
//          finalCommunities -= otherCId
//        }
//      } else {
//        // Hub is in its own community, keep the community as is
//        finalCommunities += (cId -> vertexSet)
//      }
//    }
//
//    // Remove single-member communities
//    finalCommunities = finalCommunities.filter { case (_, vertices) => vertices.size > 2}
//
//    // Log the final communities
//    logCommunities(finalCommunities)
//
//    // Print the final communities
//    println("Analytical Communities:")
//    finalCommunities.toList.sortBy(_._1).foreach { case (cId, vertices) =>
//      println(s"Community's nodeHUB $cId: [${vertices.mkString(", ")}]")
//    }
//
//    // Count distinct communities
//    val distinctCommunitiesCount = finalCommunities.values.flatten.toSet.size
//    Logger.getRootLogger.warn(s"Generated $distinctCommunitiesCount distinct communities.")
//
//    // Return the count of updated communities
//    distinctCommunitiesCount
//  }
//
////  // Analytical info for the final communities
////  def printCommunities(communityMap: VertexRDD[VertexId]): Unit = {
////    //  analytical communities with the verticies inside []
////    val communities = communityMap.map(x => (x._2, x._1)).groupByKey().sortBy(_._1, ascending = true)
////    communities.collect().foreach(x => println(s"Community's nodeHUB ${x._1}: [${x._2.mkString(", ")}]"))
////
////  }
//
//  def logCommunities(finalCommunities: Map[VertexId, Set[VertexId]]): Unit = {
//    // Get the current timestamp
//    val timestamp = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss").format(new Date())
//
//     //create the new txt file under the resultsDWCC folder
//    val logFilePath = s"resultsDWCC/communities-$timestamp.txt"
//    val logCommunities = new PrintWriter(logFilePath)
//
//     //print the analytical communities with the vertices
//    logCommunities.println("Analytical Communities:")
//    finalCommunities.toList.sortBy(_._1).foreach { case (cId, vertices) =>
//      logCommunities.println(s"${vertices.mkString(", ")}")
//    }
//
//    // add every Logger warn message as printed in the console
//    val distinctCommunitiesCount = finalCommunities.size
//    Logger.getRootLogger.warn(s"Generated $distinctCommunitiesCount communities.")
//    logCommunities.println(s"Generated $distinctCommunitiesCount distinct communities.")
//
//    logCommunities.close()
//    println(s"Communities have been logged to $logFilePath")
//  }

  private def countTriangles[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): (Graph[Float, ED], VertexRDD[(Float, Float, Float)]) = {
    val triangleScorer = new TriangleScoringWithTimeIntervals()

    // Cast edge weights to Double
    val graphWithDoubleWeights = graph.mapEdges(e => e.attr.asInstanceOf[Float])

    // Run the triangle scoring algorithm
    val scores1: VertexRDD[(Float, Float, Float)] = triangleScorer.run(graphWithDoubleWeights)

    // Extract the score part from the VertexRDD[(Double, Double)]
    val scores = scores1.mapValues(_._2)

    // Merge triangle scores back into the original graph
    val scoredGraph: Graph[Float, ED] = graph.outerJoinVertices(scores) {
      (vid, attr, optScore) => optScore.getOrElse(0f)
    }

    (scoredGraph, scores1)
  }


  /**
   * PHASE I
   * Optimize the graph by removing edges that does not close any triangles:
   * - Compute the number of triangles passing through each vertex.
   * - Get the neighbors of each vertex.
   * - Remove edges/vertices that are not part of any triangles.
   *
   * @param graph
   * @param isCanonical whether the passed `graph` is canonical or not:
   *                    The Triangle Count algorithm requires that the graph to be canonical. However,
   *                    The canonicalization procedure is costly as it requires repartitioning the graph.
   *                    The parameter `isCanonical` should be passed as true if the input data is
   *                    already in "canonical form", meaning all of the following holds:
   *                    <ul>
   *                    <li> There are no self edges</li>
   *                    <li> All edges are oriented (src is greater than dst)</li>
   *                    <li> There are no duplicate edges</li>
   *                    </ul>
   */
  def preprocess[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], isCanonical: Boolean): Graph[VertexData, ED] = {
    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")
    var before = System.currentTimeMillis()

    // Run triangle scoring and get both scores and sum of interval lengths
    val (tcGraph, scoresAndIntervalLengths) = countTriangles(graph)

    Logger.getRootLogger.warn(s"Counting Triangles took: ${System.currentTimeMillis() - before}")

    Logger.getRootLogger.warn("Phase: Preprocessing - Graph Optimization")
    before = System.currentTimeMillis()

    // Collect neighbor IDs for each vertex
    val neighborRDD = graph.collectNeighborIds(EdgeDirection.Either)
    val subGraph = tcGraph.outerJoinVertices(neighborRDD)((vertexId, CountTriangles, neighbors) => {
      (CountTriangles, neighbors.getOrElse(Array[VertexId]()))
    }).subgraph(t => {
      t.srcAttr._2.intersect(t.dstAttr._2).nonEmpty
    }, (vertexId, vData) => {
      vData._1 > 0.0

    }).partitionBy(PartitionStrategy.EdgePartition2D).cache()

    Logger.getRootLogger.warn(s"vertices: ${subGraph.vertices.count}, edges: ${subGraph.edges.count}")
    Logger.getRootLogger.warn(s"Optimization took: ${System.currentTimeMillis() - before}")

    Logger.getRootLogger.warn("Phase: Preprocessing - Saving Vertices Data")
    before = System.currentTimeMillis()

    // Update the graph with the new VertexData including sumTimeIntervalLengths and triangle scores
    val optGraph = graph.outerJoinVertices(scoresAndIntervalLengths)((vertexId, vData, scoresAndInterval) => {
      val (sumIntervalLengths, score, total) = scoresAndInterval.getOrElse((0f, 0f, 0f))
      new VertexData(vertexId, score, sumIntervalLengths, total)
    }).partitionBy(PartitionStrategy.EdgePartition2D)

    optGraph.vertices.count()
    optGraph.edges.count()
    Logger.getRootLogger.warn(s"Degree counting took: ${System.currentTimeMillis() - before}")

    subGraph.unpersist(blocking = false)
    optGraph
  }

//  def preprocess[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], isCanonical: Boolean): Graph[VertexData, ED] = {
//    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")
//    var before = System.currentTimeMillis()
//
//    // Run triangle scoring and get both scores and sum of interval lengths
//    val (tcGraph, scoresAndIntervalLengths) = countTriangles(graph)
//
//    Logger.getRootLogger.warn(s"Counting Triangles took: ${System.currentTimeMillis() - before}")
//
//    Logger.getRootLogger.warn("Phase: Preprocessing - Graph Optimization")
//    before = System.currentTimeMillis()
//
//    // Collect neighbor IDs for each vertex
//    val neighborRDD = graph.collectNeighborIds(EdgeDirection.Either)
//
//    val subGraph = tcGraph.outerJoinVertices(neighborRDD)((vertexId, countTriangles, neighbors) => {
//      (countTriangles, neighbors.getOrElse(Array.empty[VertexId]))
//    }).subgraph(
//      epred = triplet => triplet.srcAttr._2.intersect(triplet.dstAttr._2).nonEmpty,
//      vpred = (vertexId, vData) => vData._1 > 0.0
//    ).partitionBy(PartitionStrategy.EdgePartition2D).cache()
//
//    Logger.getRootLogger.warn(s"vertices: ${subGraph.vertices.count}, edges: ${subGraph.edges.count}")
//    Logger.getRootLogger.warn(s"Optimization took: ${System.currentTimeMillis() - before}")
//
//    Logger.getRootLogger.warn("Phase: Preprocessing - Saving Vertices Data")
//    before = System.currentTimeMillis()
//
//    // Instead of degrees (count of edges), compute weighted degrees (sum of incident edge weights)
//    val weightedDegrees = subGraph.aggregateMessages[Double](
//      triplet => {
//        triplet.sendToSrc(triplet.attr.asInstanceOf[Double])
//        triplet.sendToDst(triplet.attr.asInstanceOf[Double])
//      },
//      _ + _  // sum the weights
//    )
//
//    // Update the graph with VertexData
//    val optGraph = subGraph.outerJoinVertices(weightedDegrees)((vertexId, vData, weightedDegreeOpt) => {
//      val weightedDegree = weightedDegreeOpt.getOrElse(0.0)
//      println(s"VertexId: $vertexId, triangleCount: ${vData._1}, neighbors: ${vData._2.mkString("[", ", ", "]")}, weightedDegree: $weightedDegree")
//      new VertexData(vertexId, vData._1, weightedDegree)
//    }).partitionBy(PartitionStrategy.EdgePartition2D)
//
//    optGraph.vertices.count()
//    optGraph.edges.count()
//    Logger.getRootLogger.warn(s"Degree counting (weighted sum) took: ${System.currentTimeMillis() - before}")
//
//    subGraph.unpersist(blocking = false)
//    optGraph
//  }

  /**
   * PHASE II
   * Computes an initial partition of the graph:
   * - Compute the clustering coefficient of each vertex of the graph.
   * - Sort vertices by the clustering coefficient then degree in descending order.
   * - We start in a state in which all vertices are considered as not `visited`.
   * - For each non-`visited` vertex, in the calculated order, do the following:
   *     - Create a new community that contains the vertex and all its neighbors
   *       that we did not visit so far
   *     - Mark the vertex and its neighbors as `visited`.
   *       - The partition contains all the created communities.
   *
   * @param graph
   */

  def performInitialPartition[ED: ClassTag](graph: Graph[VertexData, ED], maxIterations: Int = 1): Graph[VertexData, ED] = {
    // Persist with serialization to reduce GC overhead
    graph.cache()
    val before = System.currentTimeMillis()

    val pregelGraph = graph.pregel(Map.empty[Long, VertexMessage], maxIterations)(
      vprog = (vId: VertexId, data: VertexData, messages: Map[Long, VertexMessage]) => {
        val newData = data.copy() // Could optimize if conditional copy possible
        if (messages.nonEmpty) {
          newData.changed = false
          if (messages.tail.isEmpty && messages.head._2.vId == vId) {
            // Do nothing
          } else {
            newData.neighbors = if (newData.neighbors.isEmpty) {
              (messages - vId).values.toList
            } else {
              updateNeighborsCommunities(newData, messages)
            }
            val highestNeighbor = getHighestCenterNeighbor(newData.neighbors)
            if (highestNeighbor.isDefined && VertexMessage.ordering.gt(highestNeighbor.get, VertexMessage.create(newData))) {
              newData.changed = newData.isCenter
              newData.cId = highestNeighbor.get.vId
            } else {
              newData.changed = !newData.isCenter
              newData.cId = vId
            }
          }
        } else {
          newData.changed = true
        }
        newData
      },
      sendMsg = (t: EdgeTriplet[VertexData, ED]) => {
        val messages = mutable.Map[Long, Map[Long, VertexMessage]]()
        val (to, from) = t.srcAttr.compareTo(t.dstAttr)
        if (from.changed) {
          val msg = VertexMessage.create(from)
          messages.put(from.vId, Map[Long, VertexMessage]((from.vId, msg)))
          messages.put(to.vId, Map[Long, VertexMessage]((from.vId, msg)))
        }
        messages.toIterator
      },
      mergeMsg = _ ++ _
    )

    //    // Extract initial partition and save it to a file
    //    val initialPartition = pregelGraph.vertices.collect()
    //      .groupBy { case (vId, vData) => vData.cId }
    //      .map { case (cId, vertices) => vertices.map(_._1).mkString(", ") }
    //      .mkString("\n")
    //
    //    val writer = new PrintWriter(new File("initial_partition.txt"))
    //    writer.write(initialPartition)
    //    writer.close()
    pregelGraph.vertices.count()

    Logger.getRootLogger.warn(s"Initial Partition took: ${System.currentTimeMillis() - before}")

    val partitionedGraph = pregelGraph.mapVertices((vId, vData) => {
      val data = vData.copy()
      data.changed = false
      data.neighbors = List.empty
      data
    }).partitionByCommunity(numPartitions, _.cId)

    partitionedGraph
  }

//  def performInitialPartition[ED: ClassTag](graph: Graph[VertexData, ED]): Graph[VertexData, ED] = {
//    graph.cache()
//    val before = System.currentTimeMillis()
//
//    // Print clustering coefficients at the beginning
//    graph.vertices.foreach { case (vId, vData) =>
//      println(s"Node $vId - Clustering Coefficient: ${vData.cc}")
//    }
//
//    val pregelGraph = graph.pregel(Map.empty[Long, VertexMessage])(
//      vprog = (vId: VertexId, data: VertexData, messages: Map[Long, VertexMessage]) => {
//        val newData = data.copy()
//        if (messages.nonEmpty) {
//          newData.changed = false
//          // Sender is the highest among its neighbors.
//          // Stop broadcasting the change.
//          if (messages.tail.isEmpty && messages.head._2.vId == vId) {
//            // Do nothing
//          } else {
//            // update neighbors data.
//            newData.neighbors = if (newData.neighbors.isEmpty) {
//              (messages - vId).values.toList
//            } else {
//              updateNeighborsCommunities(newData, messages)
//            }
//
//            val highestNeighbor = getHighestCenterNeighbor(newData.neighbors)
//
//            // Print the highest center neighbor for debugging
//            //println(s"Vertex $vId: Highest Center Neighbor = ${highestNeighbor.getOrElse("None")}")
//            // If the highest neighbor that is a center is higher than us.
//            // Receiver becomes a border node of that neighbor community.
//            // Broadcast the change only if it's changed from a center to a border
//            if (highestNeighbor.isDefined && VertexMessage.ordering.gt(highestNeighbor.get, VertexMessage.create(newData))) {
//              newData.changed = newData.isCenter
//              newData.cId = highestNeighbor.get.vId
//            }
//            // If no neighbor is higher than us and receiver is not a center
//            // Receiver become a center of its own community.
//            // Broadcast the change.
//            else {
//              newData.changed = !newData.isCenter
//              newData.cId = vId
//            }
//          }
//        } else {
//          newData.changed = true
//        }
//        newData
//      }, sendMsg = (t: EdgeTriplet[VertexData, ED]) => {
//        val messages = mutable.Map[Long, Map[Long, VertexMessage]]()
//        val (to, from) = t.srcAttr.compareTo(t.dstAttr)
//        if (from.changed) {
//          // node changed its community
//          // broadcast to lower neighbors and notify self to stop sending messages
//          // in case it is the highest among its neighbors.
//          val msg = VertexMessage.create(from)
//          messages.put(from.vId, Map[Long, VertexMessage]((from.vId, msg)))
//          messages.put(to.vId, Map[Long, VertexMessage]((from.vId, msg)))
//        }
//        messages.toIterator
//      }, mergeMsg = _ ++ _)
//
//    pregelGraph.vertices.count()
//    Logger.getRootLogger.warn(s"Initial Partition took: ${System.currentTimeMillis() - before}")
//
//    // Extract initial partition and save it to a file
//    val initialPartition = pregelGraph.vertices.collect()
//      .groupBy { case (vId, vData) => vData.cId }
//      .map { case (cId, vertices) => vertices.map(_._1).mkString(", ") }
//      .mkString("\n")
//
//    val writer = new PrintWriter(new File("initial_partition.txt"))
//    writer.write(initialPartition)
//    writer.close()
//
//    // Return the partitioned graph
//    val partitionedGraph = pregelGraph.mapVertices((vId, vData) => {
//      val data = vData.copy()
//      data.changed = false
//      data.neighbors = List.empty
//      data
//    }).partitionByCommunity(numPartitions, _.cId)
//
//    partitionedGraph
//  }


  /**
   * PHASE III
   * Improve on the initial partition while having an improvement in WCC that is more
   * than a threshold:
   * - Calculate the "best community movement" for each vertex in the graph.
   * - Calculate the WCC of the new partition.
   * - If the movement improve WCC, apply it to create a new partition.
   * - Repeat the previous steps while the improvement in WCC is greater than the
   * proposed threshold.
   *
   * @param graph
   * @param sc
   * @tparam ED the original edge attribute
   * @return
   */


  def refinePartition[ED: ClassTag](graph: Graph[VertexData, ED], sc: SparkContext): (Graph[VertexData, ED], Map[VertexId, CommunityData]) = {
    graph.cache()


    val globalCC = graph.vertices.map { case (vId, vData) => vData.cc }.sum / vertexCount
    // broadcasts are used to avoid network transfers all the time.
    var bestCs = sc.broadcast(computeCommunityStats(graph))
    var bestPartition = graph
    var bestWcc = computeGlobalWCC(bestPartition, bestCs)
    Logger.getRootLogger.warn(s"Global CC $globalCC")
    Logger.getRootLogger.warn(s"Initial WCC $bestWcc")

    var foundNewBestPartition = true
    var retriesLeft = maxRetries

    do {

      var before = System.currentTimeMillis()
      val movementGraph = getBestMovements(bestPartition, bestCs, globalCC.toFloat, vertexCount).cache()
      movementGraph.vertices.count()
      Logger.getRootLogger.warn(s"Movement took: ${System.currentTimeMillis() - before}")
      // calculate new global WCC
      before = System.currentTimeMillis()
      val newCs = sc.broadcast(computeCommunityStats(movementGraph))
      val skata = System.currentTimeMillis() - before
      val newWcc = computeGlobalWCC(movementGraph, newCs)
      retriesLeft -= 1
      Logger.getRootLogger.warn(s"calculate WCC took: $skata")
      Logger.getRootLogger.warn(s"New WCC ${"%.3f".format(newWcc)}")
      Logger.getRootLogger.warn(s"Initial WCC $bestWcc")
      Logger.getRootLogger.warn(s"Retries left $retriesLeft")
      ///Calculate clustering coefficients and print them
      //printClusteringCoefficients(bestPartition)
      // if the movements improve WCC apply them
      if (newWcc > bestWcc) {
        if (newWcc / bestWcc - 1 > threshold) {
          Logger.getRootLogger.warn("Resetting retries.")
          retriesLeft = maxRetries
        }
        bestPartition.unpersist(blocking = false)
        bestPartition = movementGraph.partitionByCommunity(numPartitions, _.cId).cache()
        bestWcc = newWcc
        bestCs = newCs
        bestPartition.vertices.count()
        movementGraph.unpersist(blocking = false)
      } else {
        foundNewBestPartition = false
      }

    } while (foundNewBestPartition && retriesLeft > 0)

    Logger.getRootLogger.warn(s"Best WCC ${"%.3f".format(bestWcc)}")

    (bestPartition, bestCs.value)
  }

  /**
   * reflect changes to neighbors communities.
   *
   * @param vertexData
   * @param neighbors
   * @return
   */
  private def updateNeighborsCommunities(vertexData: VertexData, neighbors: Map[Long, VertexMessage]): List[VertexMessage] = {
    vertexData.neighbors.map(vData => {
      vData.cId = neighbors.getOrElse(vData.vId, vData).cId
      vData
    })
  }

  /**
   * get the neighbor how is the highest and a center of its own community.
   *
   * @param neighbors
   * @return
   */
  private def getHighestCenterNeighbor(neighbors: List[VertexMessage]): Option[VertexMessage] = {
    neighbors.filter(_.isCenter).sorted(VertexMessage.ordering.reverse).headOption
  }

  /**
   * Gather some community statistics:
   * As they are defined in `CommunityData`
   *
   * @param graph
   * @return a map of communities and their statistics
   */
//  def computeCommunityStats[ED: ClassTag](graph: Graph[VertexData, ED]): Map[VertexId, CommunityData] = {
//    // Step 1: Calculate the size and sum of clustering coefficient (cc)
//    val communityAggregates = graph.vertices.map { case (vId, vData) =>
//      (vData.cId, (1, vData.cc)) // (communityId, (count, sum of cc))
//    }.reduceByKey { case ((count1, ccSum1), (count2, ccSum2)) =>
//      (count1 + count2, ccSum1 + ccSum2)
//    }.collectAsMap()
//
////    // Step 1.1: Collect nodes belonging to each community
////    val communityNodes = graph.vertices.map { case (vId, vData) =>
////      (vData.cId, List(vId)) // (communityId, List of node IDs)
////    }.reduceByKey(_ ++ _).collectAsMap()
////
////     //Debugging: Print community sizes and clustering coefficient sums
////    println("Community Aggregates (Size and CC Sum):")
////    communityAggregates.foreach { case (community, (size, ccSum)) =>
////    println(s"Community $community -> Size: $size, CC Sum: $ccSum")
////    }
////
////     //Debugging: Print nodes in each community
////    println("Nodes in Each Community:")
////    communityNodes.foreach { case (community, nodes) =>
////    println(s"Community $community -> Nodes: ${nodes.mkString(", ")}")
////    }
//
//    // Step 2: Calculate internal and external edges for each community
//    val communityEdges = graph.triplets.flatMap { triplet =>
//        val weight2 = triplet.attr.asInstanceOf[Float]
//
//        if (triplet.srcAttr.cId == triplet.dstAttr.cId) {
//          Iterator((("INT", triplet.srcAttr.cId), weight2))
//        } else {
//          Iterator(
//            (("EXT", triplet.srcAttr.cId), weight2),
//            (("EXT", triplet.dstAttr.cId), weight2)
//          )
//        }
//      }.reduceByKey(_ + _)
//      .collectAsMap()
//
//
//    // Debugging: Print internal and external edges for each community
//    //println("Community Edges:")
//    //communityEdges.foreach { case ((edgeType, community), count) =>
//    //println(s"Community $community -> $edgeType Edges: $count")
//    //}
//
//    // Step 3: Combine results into CommunityData
//    val communityStats = communityAggregates.map { case (community, (size, ccSum)) =>
//      val intEdges = communityEdges.getOrElse(("INT", community), 0f)
//      val extEdges = communityEdges.getOrElse(("EXT", community), 0f)
//      val avgCC = if (size > 0) ccSum / size else 0f
//      (community, new CommunityData(size, intEdges, extEdges, avgCC))
//    }.toMap
//
//    // Debugging: Print final community statistics
//    //println("Final Community Stats:")
//    //communityStats.foreach { case (community, data) =>
//    //println(s"Community $community -> Size: ${data.r}, Internal Edges: ${data.a}, " +
//    //s"External Edges: ${data.b}, Avg CC: ${data.avgCC}")
//    //}
//
//    communityStats
//  }

  private def computeCommunityStats[ED: ClassTag](
                                           graph: Graph[VertexData, ED]
                                         ): Map[VertexId, CommunityData] = {

    // Step 1: Aggregate community vertex stats (size and clustering coeff sum)
    val communityAggregatesRDD: RDD[(VertexId, (Int, Float))] = graph.vertices.map {
      case (_, vData) => (vData.cId, (1, vData.cc))
    }.reduceByKey { case ((count1, ccSum1), (count2, ccSum2)) =>
      (count1 + count2, ccSum1 + ccSum2)
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // Step 2: Aggregate internal and external edge weights by community
    val communityEdgesRDD: RDD[((String, VertexId), Float)] = graph.triplets.flatMap { triplet =>
      val weight = triplet.attr.asInstanceOf[Float]
      if (triplet.srcAttr.cId == triplet.dstAttr.cId) {
        Iterator((("INT", triplet.srcAttr.cId), weight))
      } else {
        Iterator(
          (("EXT", triplet.srcAttr.cId), weight),
          (("EXT", triplet.dstAttr.cId), weight)
        )
      }
    }.reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK)

    // Step 3: Join community vertex aggregates with internal edge weights
    val communityWithIntEdgesRDD: RDD[(VertexId, (Int, Float, Float))] = communityAggregatesRDD.leftOuterJoin(
      communityEdgesRDD.filter(_._1._1 == "INT").map { case ((_, cId), weight) => (cId, weight) }
    ).mapValues {
      case ((size, ccSum), intEdgeOpt) => (size, ccSum, intEdgeOpt.getOrElse(0f))
    }

    // Step 4: Join the above with external edge weights and compute CommunityData
    val communityStatsRDD: RDD[(VertexId, CommunityData)] = communityWithIntEdgesRDD.leftOuterJoin(
      communityEdgesRDD.filter(_._1._1 == "EXT").map { case ((_, cId), weight) => (cId, weight) }
    ).mapValues {
      case ((size, ccSum, intEdges), extEdgeOpt) =>
        val extEdges = extEdgeOpt.getOrElse(0f)
        val avgCC = if (size > 0) ccSum / size else 0f
        new CommunityData(size, intEdges, extEdges, avgCC)
    }

    // Step 5: Collect final stats map to driver (should be manageable size)
    communityStatsRDD.collectAsMap().toMap
  }

  /**
   * calculates the best movements for all vertices in a partition.
   *
   * @param graph
   * @param bCommunityStats a broadcast of community statistics
   * @param globalCC        the global clustering coefficient
   * @param vertexCount     the count of vertices in the graph
   * @tparam ED
   * @return
   */
  private def getBestMovements[ED: ClassTag](
                                              graph: Graph[VertexData, ED],
                                              bCommunityStats: Broadcast[Map[VertexId, CommunityData]],
                                              globalCC: Float,
                                              vertexCount: Long
                                            ): Graph[VertexData, ED] = {

    val vertexCommunityDegrees = graph.aggregateMessages[Map[VertexId, Float]](
      ctx => {
        // ctx.attr already contains the edge weight
        val weight: Float = ctx.attr.asInstanceOf[Float]

        ctx.sendToDst(Map(ctx.srcAttr.cId -> weight))
        ctx.sendToSrc(Map(ctx.dstAttr.cId -> weight))
      },
      (a: Map[VertexId, Float], b: Map[VertexId, Float]) => {
        // Merge the two maps, summing values for duplicate keys (VertexId)
        a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0f)) }
      }
    )

    graph.outerJoinVertices(vertexCommunityDegrees)((vId, vertex, vcDegrees) => {
      bestMovement(vertex, vcDegrees.get, bCommunityStats.value, globalCC, vertexCount)
    })
  }



  /**
   * Implementation of the "bestMovement" algorithm:
   * For each vertex of the graph we choose the movement that improves the WCC of
   * the partition the most. There are three types of possible movements:
   *  - Transfer: The vertex moves from its community to the community of a
   *    neighboring vertex.
   *  - Remove: The vertex removes itself from its current community and becomes
   *    the sole member of its own.
   *  - Stay: The vertex remains in its current community.
   *
   * @param vertex         the vertex to move
   * @param vcDegrees      map of communities adjacent to vertex `vId` and the count of
   *                       edge connected them to vertex `vId`
   * @param communityStats community statistics
   * @param globalCC       the graph average clustering coeficient value
   * @param vertexCount    the vertices count
   * @return an updated statistics of vertex `vId`
   */
  private def bestMovement(vertex: VertexData, vcDegrees: Map[VertexId, Float],
                           communityStats: Map[VertexId, CommunityData],
                           globalCC: Float, vertexCount: Long): VertexData = {
    val newVertex = vertex.copy()

    // WCCR(v,C) computes the improvement of the WCC of a partition when
    // a vertex v is removed from community C and placed in its own isolated community.
    val wccR = computeWccR(vertex, vcDegrees, communityStats(vertex.cId), globalCC, vertexCount)
    // WCCT (v,C1,C2) computes the improvement of the WCC of a partition when vertex
    // v is transferred from community C1 and to C2.
    var wccT = 0f
    var bestC = vertex.cId
    vcDegrees.foreach { case (cId, dIn) =>
      val cData = communityStats(cId)
      if (vertex.cId != cId && cData.r > 1) {
        val dOut = vcDegrees.values.sum - dIn
        val candidateWccT = wccR + WCCMetric.computeWccI(cData, dIn, dOut, globalCC, vertexCount)
        if (candidateWccT > wccT) {
          wccT = candidateWccT
          bestC = cId
        }
      }
    }

    // m ← [REMOVE];
    if (wccR - wccT > 0.00001 && wccR > 0f) {
      newVertex.cId = vertex.vId
    }
    // m ← [TRANSFER , bestC];
    else if (wccT > 0f) {
      newVertex.cId = bestC
    }
    // m ← [STAY];

    newVertex
  }

  /**
   * Calculates the approximation of the change to the global wcc that
   * would be caused by removing this vertex from its current community
   * and isolated.
   */
  private def computeWccR(vData: VertexData, vcDegrees: Map[VertexId, Float],
                          cData: CommunityData, globalCC: Float, vertexCount: Long): Float = {
    // if vertex is isolated
    if (cData.r == 1) return 0f

    // Ensure dIn is a Double
    val dIn = vcDegrees.getOrElse(vData.cId, 0f) // Using 0.0 instead of 0 to ensure Double

    // Ensure vcDegrees.values is summed correctly
    val dOut = vcDegrees.values.sum // This will now correctly sum the Double values

    // Create a new CommunityData with the vertex removed
    val cDataWithVertexRemoved = new CommunityData(
      cData.r - 1,
      cData.a - dIn,
      cData.b + dIn - dOut,
      cData.avgCC - vData.cc / cData.r
    )

    // Compute the WCC value using the updated community data
    -WCCMetric.computeWccI(cDataWithVertexRemoved, dIn, dOut, globalCC, vertexCount)
  }


  /**
   * Update the graph vertices with their new WCC values in respect to their current communities.
   *
   * @param graph
   * @param bCommunityStats
   * @tparam ED the original edge attribute
   * @return
   */
  private def computeGlobalWCC[ED: ClassTag](graph: Graph[VertexData, ED], bCommunityStats: Broadcast[Map[VertexId, CommunityData]]): Float = {
    // Collect neighbor IDs with edge attributes
    val communityNeighborIdsWithEdgeAttrs = collectCommunityNeighborIds(graph)

    // Create community neighbor graph with the new structure
    val communityNeighborGraph = graph.outerJoinVertices(communityNeighborIdsWithEdgeAttrs)((vId, vData, neighbours) => {
      val neighbors = neighbours.getOrElse(Array.empty[(VertexId, (VertexId, ED))])
      (vData, neighbors)
    }).cache()

    // Convert edge type to Double and vertex attributes to Map[VertexId, Double]
    val convertedGraph = communityNeighborGraph.mapEdges { edge =>
      // Cast the edge data to Double
      edge.attr.asInstanceOf[Float]
    }.mapVertices { case (vId, (vData, arr)) =>
      val neighborMap: Map[VertexId, Float] = arr.map {
        case (nbr, (_, weight)) => (nbr, weight.asInstanceOf[Float])
      }.toMap
      (vData, neighborMap)
    }

    // Now you can safely call countCommunityTriangles with the converted graph
    val trianglesAndIntervalLengths = countCommunityTriangles(convertedGraph)


    // Compute the WCC metric for each vertex
    val wccResult = communityNeighborGraph.outerJoinVertices(trianglesAndIntervalLengths)((vId, data, tC) => {
      val (sumTimeIntervalLengths, counter) = tC.getOrElse((0f, 0f))
      WCCMetric.computeWccV(data._1, bCommunityStats.value(data._1.cId), sumTimeIntervalLengths, counter)
    })

    // Compute the sum of WCC values and divide by the vertex count
    val vertexCount = graph.vertices.count() // Convert to Double for accurate division
    val averageWCC = wccResult.vertices.map { case (vId, wcc) => wcc }.sum / vertexCount // Compute average WCC

    averageWCC.toFloat // Return the computed average WCC as a Double
  }

  private def collectCommunityNeighborIds[VD: ClassTag, ED: ClassTag](graph: Graph[VertexData, ED]): VertexRDD[Array[(VertexId, (VertexId, ED))]] = {
    graph.aggregateMessages(ctx => {
      // Check if the destination vertex attribute matches the source vertex attribute
      // This assumes vertices with the same attribute are part of the same community
      if (ctx.dstAttr.cId == ctx.srcAttr.cId) {
        // Send message to destination vertex with source ID and edge attribute
        ctx.sendToDst(Array((ctx.srcId, (ctx.dstId, ctx.attr))))
        // Send message to source vertex with destination ID and edge attribute
        ctx.sendToSrc(Array((ctx.dstId, (ctx.srcId, ctx.attr))))
      }
    }, _ ++ _)
  }


  private def countCommunityTriangles[ED: ClassTag](
                                                     graph: Graph[(VertexData, Map[VertexId, Float]), Float]
                                                   ): VertexRDD[(Float, Float)] = {

    val weightedTriangleScores: VertexRDD[(Float, Float)] = graph.aggregateMessages[(Float, Float)](
      ctx => {
        // Only proceed if both vertices are in the same community
        if (ctx.srcAttr._1.cId == ctx.dstAttr._1.cId) {
          val srcMap = ctx.srcAttr._2
          val dstMap = ctx.dstAttr._2

          val (smaller, larger) =
            if (srcMap.size < dstMap.size) (srcMap, dstMap) else (dstMap, srcMap)

          val allNeighbors = (smaller.keySet ++ larger.keySet) - ctx.srcId - ctx.dstId
          val commonNeighbors = smaller.keySet.intersect(larger.keySet) - ctx.srcId - ctx.dstId
          val commonNeighborsSize = commonNeighbors.size

          var triangleScore = 0f
          var sumTimeIntervalLengths = 0f

          for (nbr <- allNeighbors) {
            val w1 = ctx.attr

            if (commonNeighbors.contains(nbr)) {
              val w2 = smaller(nbr)
              val w3 = larger(nbr)
              val tri = (w1 + w2 + w3) / 3f
              triangleScore += tri
              sumTimeIntervalLengths += tri / commonNeighborsSize // Avoid divide by zero
            }
          }

          ctx.sendToSrc((sumTimeIntervalLengths, triangleScore / 2f))
          ctx.sendToDst((sumTimeIntervalLengths, triangleScore / 2f))
        }
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )


    weightedTriangleScores
  }

}


