package wcc

import communityDetection.TriangleScoringWithTimeIntervals

import java.io.{File, IOException, PrintWriter}
import java.nio.file.{Paths, Files}
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.collection.mutable
import scalaz.Scalaz._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{VertexRDD, _}
import wcc.GraphXOps.GXOperations

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Implementation of the "t-iWCC" algorithm.
 */
object DistributedWCC {

  // Main parameters
  private val threshold = 0.01f         // default threshold 0.01
  private var numPartitions = 200 // default number of partitions 200
  var vertexCount = 0
  // Main parameters end
 

  def runWithStats[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext, queryTimeInterval: (Int, Int),
                                               maxRetries: Int = this.maxRetries,
                                               partitions: Int = this.numPartitions,
                                               isCanonical: Boolean = false, queryTimeIntervalLength: Int
                                              ): (Graph[VertexData, ED], Map[VertexId, CommunityData]) = {
    require(maxRetries > 0, s"Number of iterations must be greater than or equal to 0," +
      s" but got $maxRetries")
    require(partitions > 0 , s"Number of partitions must be greater than 0," +
      s" but got $partitions")

    this.maxRetries = maxRetries
    this.numPartitions = partitions
    this.vertexCount = graph.vertices.count.toInt

    val before = System.currentTimeMillis()

    val optimizedGraph = preprocess(graph, isCanonical, queryTimeIntervalLength)
    val initGraph = performInitialPartition(optimizedGraph)
    val (communityGraph, cStats) = refinePartition(initGraph, queryTimeInterval: (Int, Int), queryTimeIntervalLength, sc)

    Logger.getRootLogger.warn(s"Took ${"%.3f".format((System.currentTimeMillis() - before) / 1000.0)} seconds.")

    val dataGraph = graph.outerJoinVertices(communityGraph.vertices)((vId, _, vDataOpt) =>
      vDataOpt.getOrElse(new VertexData(vId, 0, 0, 0f))
    )

    (dataGraph, cStats)
  }

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      queryTimeInterval: (Int, Int),
                                      queryTimeIntervalLength: Int,
                                      sc: SparkContext,
                                      logFilePath: String,
                                      maxRetries: Int = this.maxRetries,
                                      partitions: Int = this.numPartitions,
                                      isCanonical: Boolean = false
                                     ): VertexRDD[VertexId] = {
    require(maxRetries >= 0, s"Number of iterations must be greater than or equal to 0," +
      s" but got $maxRetries")
    require(partitions > 0 , s"Number of partitions must be greater than 0," +
      s" but got $partitions")

    this.maxRetries = maxRetries
    this.numPartitions = partitions
    this.vertexCount = graph.vertices.count.toInt

    val startTime = System.nanoTime()

    Logger.getRootLogger.warn("Phase: Preprocessing Start...")
    val optimizedGraph = preprocess(graph, isCanonical, queryTimeIntervalLength)

    Logger.getRootLogger.warn("Phase: Community Initialization Start...")
    val initGraph = performInitialPartition(optimizedGraph)
//    val initCommunityMap = initGraph.vertices.mapValues((vId, vData) => vData.cId)
//    printStats(initCommunityMap)

    Logger.getRootLogger.warn("Phase: WCC Iteration Start...")
    val communityMap = refinePartition(initGraph, queryTimeInterval: (Int, Int), queryTimeIntervalLength, sc)._1.vertices.mapValues((vId, vData) => vData.cId)

    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9  // Convert nanoseconds to seconds

    //Logger.getRootLogger.warn(s"Took ${"%.3f".format((System.currentTimeMillis() - before) / 1000.0)} seconds.")
//    // Compute community statistics
//    val communityStats = computeCommunityStats(initGraph, queryTimeInterval, queryTimeIntervalLength)
//
//    // Log the computed community statistics
//    Logger.getRootLogger.warn("Computed Community Statistics:")
//    communityStats.foreach { case (community, data) =>
//      Logger.getRootLogger.warn(s"Community $community -> Size: ${data.r}, " +
//        s"Internal Edges: ${data.a}, External Edges: ${data.b}, Avg CC: ${data.avgCC}")
//    }
    printStats(communityMap)
    printCommunities(communityMap,logFilePath,elapsedTime)  // print the analytical communities with the vertices inside
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

  def logCommunities(formattedCommunities: Seq[String], logFilePath: String, elapsedTime:Double): Unit = {
    // Get the current timestamp (optional: if you want to add timestamp to filename here)
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


  // method that counts the number of triangles each vertex is part of
  private def countTriangles[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],queryTimeIntervalLength: Int): (Graph[Float, ED],  VertexRDD[(Float, Float, Float)]) = {
    val triangleScorer = new TriangleScoringWithTimeIntervals()

    // Run the triangle scoring algorithm
    val scoresAndIntervalLengths: VertexRDD[(Float, Float, Float)] = triangleScorer.run(queryTimeIntervalLength, graph.asInstanceOf[Graph[Int, (Int, Int)]])

    // Extract the score part from the VertexRDD[(Double, Double)]
    val scores = scoresAndIntervalLengths.mapValues(_._2)

    // Merge triangle scores back into the original graph
    val scoredGraph: Graph[Float, ED] = graph.outerJoinVertices(scores) {
      (vid, attr, optScore) => optScore.getOrElse(0f)
    }

    (scoredGraph, scoresAndIntervalLengths)
  }

  /**
   * PHASE I
   * Optimize the graph by removing edges that does not close any triangles:
   * - Compute the contribution of triangles
   * - Get the neighbors of each vertex.
   * - Remove edges/vertices that are not part of any triangles.
   */
  def preprocess[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], isCanonical: Boolean, queryTimeIntervalLength: Int): Graph[VertexData, ED] = {
    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")
    var before = System.currentTimeMillis()

    // Run triangle scoring and get both scores and sum of interval lengths
    val (tcGraph, scoresAndIntervalLengths) = countTriangles(graph, queryTimeIntervalLength)

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



  /**
   * PHASE II
   * Computes an initial partition of the graph:
   * - Compute the temporal local clustering coefficient of each vertex of the graph.
   * - Sort vertices by the temporal local clustering coefficient in descending order.
   * - We start in a state in which all vertices are considered as not `visited`.
   * - For each non-`visited` vertex, in the calculated order, do the following:
   *     - Create a new community that contains the vertex and all its neighbors
   *       that we did not visit so far
   *     - Mark the vertex and its neighbors as `visited`.
   * - The partition contains all the created communities.
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

  /**
   * PHASE III
   * Improve on the initial partition while having an improvement in WCC that is more
   * than a threshold:
   * - Calculate the "best community movement" for each vertex in the graph.
   * - Calculate the WCC of the new partition.
   * - If the movement improve WCC, apply it to create a new partition.
   * - Repeat the previous steps while the improvement in WCC is greater than the
   *   proposed threshold.
   * @param graph
   * @param sc
   * @tparam ED the original edge attribute
   * @return
   */




  def refinePartition[ED: ClassTag](graph: Graph[VertexData, ED], queryTimeInterval: (Int, Int), queryTimeIntervalLength: Int,sc: SparkContext): (Graph[VertexData, ED], Map[VertexId, CommunityData]) = {
    graph.cache()


    val globalCC = graph.vertices.map { case (vId, vData) => vData.cc }.sum / vertexCount
    // broadcasts are used to avoid network transfers all the time.
    var bestCs = sc.broadcast(computeCommunityStats(graph, queryTimeInterval, queryTimeIntervalLength))
    var bestPartition = graph
    var bestWcc = computeGlobalWCC(bestPartition, queryTimeIntervalLength, bestCs)
    Logger.getRootLogger.warn(s"Global CC $globalCC")
    Logger.getRootLogger.warn(s"Initial WCC $bestWcc")

    var foundNewBestPartition = true
    var retriesLeft = maxRetries

    do {

      var before = System.currentTimeMillis()
      val movementGraph = getBestMovements(bestPartition, queryTimeInterval: (Int, Int), queryTimeIntervalLength, bestCs, globalCC.toFloat, vertexCount).cache()
      movementGraph.vertices.count()
      Logger.getRootLogger.warn(s"Movement took: ${System.currentTimeMillis() - before}")
      // calculate new global WCC
      before = System.currentTimeMillis()
      val newCs = sc.broadcast(computeCommunityStats(movementGraph, queryTimeInterval, queryTimeIntervalLength))
      val skat = System.currentTimeMillis() - before
      val newWcc = computeGlobalWCC(movementGraph, queryTimeIntervalLength, newCs)
      Logger.getRootLogger.warn(s"calculate WCC took: $skat")
      Logger.getRootLogger.warn(s"New WCC $newWcc")
      Logger.getRootLogger.warn(s"Initial WCC $bestWcc")
      Logger.getRootLogger.warn(s"Retries left $retriesLeft")
      // if the movements improve WCC apply them
      if (newWcc > bestWcc) {
        if (newWcc / bestWcc - 1 > threshold) {
          Logger.getRootLogger.warn("Resetting retries.")
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
   * @param graph
   * @return a map of communities and their statistics
   */
  def computeCommunityStats[ED: ClassTag](
                                           graph: Graph[VertexData, ED],
                                           queryTimeInterval: (Int, Int),
                                           queryTimeIntervalLength: Int
                                         ): Map[VertexId, CommunityData] = {

    val (queryStart, queryEnd) = queryTimeInterval

    // Step 1: Compute community aggregates (size, sum of CC)
    val communityAggregatesRDD = graph.vertices
      .map { case (_, vData) => (vData.cId, (1, vData.cc)) }
      .reduceByKey { case ((count1, ccSum1), (count2, ccSum2)) =>
        (count1 + count2, ccSum1 + ccSum2)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Step 2: Compute internal and external edge weights based on time overlap
    val communityEdgesRDD = graph.triplets.flatMap { triplet =>
        val (attrStart, attrEnd) = triplet.attr.asInstanceOf[(Int, Int)]
        val start = math.max(attrStart, queryStart)
        val end = math.min(attrEnd, queryEnd)

        if (start > end) Iterator.empty
        else {
          val weight = (end - start + 1f) / queryTimeIntervalLength
          val srcC = triplet.srcAttr.cId
          val dstC = triplet.dstAttr.cId

          if (srcC == dstC)
            Iterator((("INT", srcC), weight))
          else
            Iterator((("EXT", srcC), weight), (("EXT", dstC), weight))
        }
      }.reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Step 3: Join aggregates with internal edges
    val withInternalEdges = communityAggregatesRDD.leftOuterJoin(
      communityEdgesRDD.filter(_._1._1 == "INT").map { case ((_, cId), weight) => (cId, weight) }
    ).mapValues {
      case ((size, ccSum), intWOpt) => (size, ccSum, intWOpt.getOrElse(0f))
    }

    // Step 4: Join with external edges, compute CommunityData
    val communityStatsRDD = withInternalEdges.leftOuterJoin(
      communityEdgesRDD.filter(_._1._1 == "EXT").map { case ((_, cId), weight) => (cId, weight) }
    ).mapValues {
      case ((size, ccSum, intW), extWOpt) =>
        val extW = extWOpt.getOrElse(0f)
        val avgCC = if (size > 0) ccSum / size else 0f
        new CommunityData(size, intW, extW, avgCC)
    }

    // Step 5: Collect to driver as final result
    communityStatsRDD.collectAsMap().toMap
  }


  /**
   * calculates the best movements for all vertices in a partition.
   * @param graph
   * @param bCommunityStats a broadcast of community statistics
   * @param globalCC the global clustering coefficient
   * @param vertexCount the count of vertices in the graph
   * @tparam ED
   * @return
   */
  private def getBestMovements[ED: ClassTag](graph: Graph[VertexData, ED], queryTimeInterval: (Int, Int), queryTimeIntervalLength: Int,
                                             bCommunityStats: Broadcast[Map[VertexId, CommunityData]],
                                             globalCC: Float, vertexCount: Int): Graph[VertexData, ED] = {
    // Compute the sum of weights for each vertex, where weight is scaled by the queryTimeIntervalLength
    val vertexCommunityDegrees = graph.aggregateMessages[Map[VertexId, Float]](
      ctx => {
        // Explicitly cast edge attributes to (Int, Int)
        val (startTime, endTime) = ctx.attr.asInstanceOf[(Int, Int)]

        val start = Math.max(startTime, queryTimeInterval._1)
        val end = Math.min(endTime, queryTimeInterval._2)

          val weight: Float = (end - start + 1f) / queryTimeIntervalLength
          //println(weight)
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
   */
  private def bestMovement(vertex: VertexData, vcDegrees: Map[VertexId, Float],
                           communityStats: Map[VertexId, CommunityData],
                           globalCC: Float, vertexCount: Int): VertexData = {
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
    if (wccR - wccT > 0.00001 && wccR > 0.0d) {
      newVertex.cId = vertex.vId
    }
    // m ← [TRANSFER , bestC];
    else if (wccT > 0.0d) {
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
                          cData: CommunityData, globalCC: Float, vertexCount: Int): Float = {
    // if vertex is isolated
    if (cData.r == 1) return 0f

    // Ensure dIn is a Double
    val dIn = vcDegrees.getOrElse(vData.cId, 0f) // Using 0.0 instead of 0 to ensure Double

    // Ensure vcDegrees.values is summed correctly
    val dOut = vcDegrees.values.sum  // This will now correctly sum the Double values

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
   * @param graph
   * @param bCommunityStats
   * @tparam ED the original edge attribute
   * @return
   */
  private def computeGlobalWCC[ED: ClassTag](graph: Graph[VertexData, ED], queryTimeIntervalLength: Int, bCommunityStats: Broadcast[Map[VertexId, CommunityData]]): Float = {
    // Collect neighbor IDs with edge attributes
    val communityNeighborIdsWithEdgeAttrs = collectCommunityNeighborIds(graph)

    // Create community neighbor graph with the new structure
    val communityNeighborGraph = graph.outerJoinVertices(communityNeighborIdsWithEdgeAttrs)((vId, vData, neighbours) => {
      val neighbors = neighbours.getOrElse(Array.empty[(VertexId, (VertexId, ED))])
      (vData, neighbors)
    }).cache()

    // Count triangles by community and get the sum of time interval lengths
    val trianglesAndIntervalLengths = countCommunityTriangles(communityNeighborGraph, queryTimeIntervalLength)

    // Compute the WCC metric for each vertex
    val wccResult = communityNeighborGraph.outerJoinVertices(trianglesAndIntervalLengths)((vId, data, tC) => {
      val (sumTimeIntervalLengths, counter) = tC.getOrElse((0f, 0f))
      WCCMetric.computeWccV(data._1, bCommunityStats.value(data._1.cId), sumTimeIntervalLengths, counter)
    })

    // Compute the sum of WCC values and divide by the vertex count
    val vertexCount = graph.vertices.count()  // Convert to Double for accurate division
    val averageWCC = wccResult.vertices.map { case (vId, wcc) => wcc }.sum / vertexCount  // Compute average WCC

    averageWCC.toFloat  // Return the computed average WCC as a Double

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

  def intersectIntervals(edge1TimeInterval: (Int, Int), queryTimeInterval: (Int, Int)): Option[(Int, Int)] = {
    val start = Math.max(edge1TimeInterval._1, queryTimeInterval._1)
    val end = Math.min(edge1TimeInterval._2, queryTimeInterval._2)
    if (start <= end) {
      Some((start, end))
    } else {
      None
    }
  }

  private def countCommunityTriangles[ED: ClassTag](
                                                     graph: Graph[(VertexData, Array[(VertexId, (VertexId, ED))]), ED],
                                                     queryTimeIntervalLength: Int
                                                   ): VertexRDD[(Float, Float)] = {
    graph.aggregateMessages[(Float, Float)](ctx => {
      if (ctx.srcAttr._1.cId == ctx.dstAttr._1.cId) {

        // Convert neighbor arrays to sets once to reduce duplication
        val (smallArr, largeArr) =
          if (ctx.srcAttr._2.length < ctx.dstAttr._2.length) (ctx.srcAttr._2, ctx.dstAttr._2)
          else (ctx.dstAttr._2, ctx.srcAttr._2)

        // Use Sets of keys for intersection
        val smallKeys = smallArr.map(_._1).toSet
        val largeKeys = largeArr.map(_._1).toSet
        val commonElements = smallKeys.intersect(largeKeys)

        val srcDstEdgeTimeInterval = ctx.attr.asInstanceOf[(Int, Int)]

        var sumTimeIntervalLengths: Float = 0f
        var counter: Float = 0f

        // Instead of foreach, use iterator + while loop (less closure overhead)
        val iter = commonElements.iterator
        while (iter.hasNext) {
          val vId = iter.next()

          // Linear search smallArr for vId (manual to avoid .find closure allocation)
          var edge1Idx = 0
          var edge1Opt: Option[(VertexId, (VertexId, ED))] = None
          while (edge1Idx < smallArr.length && edge1Opt.isEmpty) {
            if (smallArr(edge1Idx)._1 == vId) edge1Opt = Some(smallArr(edge1Idx))
            edge1Idx += 1
          }

          // Linear search largeArr for vId
          var edge2Idx = 0
          var edge2Opt: Option[(VertexId, (VertexId, ED))] = None
          while (edge2Idx < largeArr.length && edge2Opt.isEmpty) {
            if (largeArr(edge2Idx)._1 == vId) edge2Opt = Some(largeArr(edge2Idx))
            edge2Idx += 1
          }

          (edge1Opt, edge2Opt) match {
            case (Some(edge1), Some(edge2)) =>
              val (_, (_, timeInterval1Any)) = edge1
              val (_, (_, timeInterval2Any)) = edge2

              val (start1, end1) = timeInterval1Any.asInstanceOf[(Int, Int)]
              val (start2, end2) = timeInterval2Any.asInstanceOf[(Int, Int)]
              val (startSrcDst, endSrcDst) = srcDstEdgeTimeInterval

              val start = Math.max(Math.max(start1, start2), startSrcDst)
              val end = Math.min(Math.min(end1, end2), endSrcDst)

              if (start <= end) {
                val intersect = (endSrcDst - startSrcDst + 1).toFloat / queryTimeIntervalLength
                sumTimeIntervalLengths += intersect

                val triangleScore = (end - start + 1).toFloat / queryTimeIntervalLength
                counter += triangleScore
              }
            case _ => // missing edge; skip
          }
        }

        // Divide sumTimeIntervalLengths by commonElements.size once here if needed
        val normalizedSum = if (commonElements.nonEmpty) sumTimeIntervalLengths / commonElements.size else 0f

        ctx.sendToSrc((normalizedSum, counter / 2))
        ctx.sendToDst((normalizedSum, counter / 2))
      }
    }, (a, b) => (a._1 + b._1, a._2 + b._2))
  }

}


