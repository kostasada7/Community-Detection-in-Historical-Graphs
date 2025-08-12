package communityDetection

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


class TriangleScoringWithTimeIntervals1 extends Serializable {
  def run(queryTimeInterval: (Long, Long), graph: Graph[Int, (Long, Long)]): VertexRDD[(Double, Double, Double)] = {
    runPreProcessed(graph, queryTimeInterval)
  }

  private def runPreProcessed(graph: Graph[Int, (Long, Long)], queryTimeInterval: (Long, Long)): VertexRDD[(Double, Double, Double)] = {
    val queryTimeIntervalLength: Long = queryTimeInterval._2 - queryTimeInterval._1 + 1

    // Efficiently create a neighbor set along with the time intervals for each vertex
    val nbrSets: VertexRDD[Map[VertexId, (Long, Long)]] = {
      graph.aggregateMessages[Map[VertexId, (Long, Long)]](
        triplet => {
          triplet.sendToSrc(Map(triplet.dstId -> triplet.attr))
          triplet.sendToDst(Map(triplet.srcId -> triplet.attr))
        },
        (a, b) => a ++ b
      )
    }

    // Join the graph with the neighbor sets
    val setGraph: Graph[Map[VertexId, (Long, Long)], (Long, Long)] = graph.outerJoinVertices(nbrSets) {
      (_, _, optSet) => optSet.getOrElse(Map.empty)
    }

    def intersectIntervals(edge1TimeInterval: (Long, Long), queryTimeInterval: (Long, Long)): Option[(Long, Long)] = {
      val start = Math.max(edge1TimeInterval._1, queryTimeInterval._1)
      val end = Math.min(edge1TimeInterval._2, queryTimeInterval._2)
      if (start <= end) Some((start, end)) else None
    }

    def edgeFunc(ctx: EdgeContext[Map[VertexId, (Long, Long)], (Long, Long), (Double, Double, Double)], queryTimeIntervalLength: Long): Unit = {
      val (smallSet, largeSet) = if (ctx.srcAttr.size <= ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }

      val iter = (smallSet ++ largeSet).keys.iterator
      var Total: Double = 0.0
      var Total2: Double = 0.0
      var Total1: Double = 0.0
      var score: Double = 0.0
      var sumTimeIntervalLengths: Double = 0.0

      val commonKeys = smallSet.keySet.intersect(largeSet.keySet)
      val commonKeysSize = commonKeys.size

      while (iter.hasNext) {
        val commonVertex = iter.next()
        val ena = ctx.srcAttr.get(commonVertex)
        val dyo = ctx.dstAttr.get(commonVertex)

        if (ena.isDefined && ctx.dstId != commonVertex) {
          val interSrcCommonOpt = intersectIntervals(ctx.srcAttr(commonVertex), ctx.srcAttr(ctx.dstId))

          if (dyo.isDefined && interSrcCommonOpt.isDefined && ctx.srcId != commonVertex) {
            val finalIntersectSrcCommonDstCommonOpt = intersectIntervals(interSrcCommonOpt.get, ctx.dstAttr(commonVertex))
            if (finalIntersectSrcCommonDstCommonOpt.isDefined) {
              val intersectLength = finalIntersectSrcCommonDstCommonOpt.get._2 - finalIntersectSrcCommonDstCommonOpt.get._1 + 1
              val triangleScore = intersectLength.toDouble / queryTimeIntervalLength.toDouble
              score += 1
              sumTimeIntervalLengths += triangleScore / commonKeysSize
              Total1 += triangleScore / 2
            } else {
              val intersectLengths = interSrcCommonOpt.get._2 - interSrcCommonOpt.get._1 + 1
              Total2 += intersectLengths / (2 * queryTimeIntervalLength.toDouble)
            }
          } else if (dyo.isDefined && ctx.srcId != commonVertex) {
            val interSrcCommonOpt11 = intersectIntervals(ctx.dstAttr(commonVertex), ctx.srcAttr(ctx.dstId))
            if (interSrcCommonOpt11.isDefined) {
              val intersectLength111 = interSrcCommonOpt11.get._2 - interSrcCommonOpt11.get._1 + 1
              Total += intersectLength111 / (2 * queryTimeIntervalLength.toDouble)
            }
          } else if (interSrcCommonOpt.isDefined) {
            val intersectLength111 = interSrcCommonOpt.get._2 - interSrcCommonOpt.get._1 + 1
            Total2 += intersectLength111 / (2 * queryTimeIntervalLength.toDouble)
          }
        } else if (dyo.isDefined && ctx.srcId != commonVertex) {
          val interSrcCommonOpt1 = intersectIntervals(ctx.dstAttr(commonVertex), ctx.srcAttr(ctx.dstId))
          if (interSrcCommonOpt1.isDefined) {
            val intersectLength1 = interSrcCommonOpt1.get._2 - interSrcCommonOpt1.get._1 + 1
            Total += intersectLength1 / (2 * queryTimeIntervalLength.toDouble)
          }
        }
      }

      ctx.sendToSrc((sumTimeIntervalLengths, score/2, Total1 + Total2))
      ctx.sendToDst((sumTimeIntervalLengths, score/2, Total1 + Total))
    }

    // Aggregate both triangle scores and collected time interval lengths
    val scoresAndIntervalLengths: VertexRDD[(Double, Double, Double)] = setGraph.aggregateMessages[(Double, Double, Double)](
      ctx => edgeFunc(ctx, queryTimeIntervalLength),
      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    )

    scoresAndIntervalLengths
  }
}

object TriangleScoringWithTimeIntervals1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TriangleScoringWithTimeIntervals1").setMaster("local[*]")
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    Logger.getRootLogger.warn("We have context!!")

    // Read the file and skip header lines
    val lines = sc.textFile("data/dblp111.txt")
    val edges = lines
      .filter(line => !line.startsWith("#")) // Skip header lines
      .map { line =>
        val fields = line.split("\t")
        val srcId = fields(0).toLong
        val dstId = fields(1).toLong
        val attr1 = fields(2).toLong
        val attr2 = fields(3).toLong
        Edge(srcId, dstId, (attr1, attr2))
      }.cache

    Logger.getRootLogger.warn("edges loaded!!")

    // Define the time interval
    val queryTimeInterval: (Long, Long) = (2, 4)

    // Filter the edges based on the query time interval
    val filteredEdges = edges.filter { edge =>
      val (attr1, attr2) = edge.attr
      val start = Math.max(attr1, queryTimeInterval._1)
      val end = Math.min(attr2, queryTimeInterval._2)
      start <= end
    }


    // Create the subgraph directly from the filtered edges
    val subgraph = Graph.fromEdges(filteredEdges, defaultValue = 1)
    Logger.getRootLogger.warn(s"vertices: ${subgraph.vertices.count}, edges: ${subgraph.edges.count}")
    // Run the Triangle Scoring algorithm
    // Record the start time
    val startTime = System.currentTimeMillis()
    val triangleScorer = new TriangleScoringWithTimeIntervals1()
    val result: VertexRDD[(Double, Double, Double)] = triangleScorer.run(queryTimeInterval, subgraph)

    // Sum all the scores for the vertices
    val totalScore = result.map(_._2._2).reduce(_ + _)/3  // Sum all the `score` values

    // Print the results
    //result.collect().foreach { case (vertexId, (sumIntervals, score, total)) =>
    //  println(s"Vertex: $vertexId, Sum of Intervals: $sumIntervals, Score: $score, Total: $total")
   // }

    // Record the end time
    val endTime = System.currentTimeMillis()

    // Calculate the elapsed time
    val elapsedTime = endTime - startTime
    println(s"Time taken to calculate triangle scores: $elapsedTime milliseconds")
    // Print the total score across all vertices
    println(s"Total Score for all vertices: $totalScore")

    // Stop SparkContext
    sc.stop()
    spark.stop()
  }
}

