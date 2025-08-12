//package communityDetection
//
//import org.apache.spark.graphx._
//import scala.reflect.ClassTag
//
//class TriangleScoringWithTimeIntervals extends Serializable {
//  def run(queryTimeIntervalLength: Int, graph: Graph[Int, (Int, Int)]): VertexRDD[(Float, Float, Float)] = {
//    runPreProcessed(graph, queryTimeIntervalLength)
//  }
//
//  private def runPreProcessed(graph: Graph[Int, (Int, Int)], queryTimeIntervalLength: Int): VertexRDD[(Float, Float, Float)] = {
//    // Efficiently create a neighbor set along with the time intervals for each vertex
//    val nbrSets: VertexRDD[Map[VertexId, (Int, Int)]] = {
//      graph.aggregateMessages[Map[VertexId, (Int, Int)]](
//        triplet => {
//          triplet.sendToSrc(Map(triplet.dstId -> triplet.attr))
//          triplet.sendToDst(Map(triplet.srcId -> triplet.attr))
//        },
//        (a, b) => a ++ b
//      )
//    }
//
//    // Join the graph with the neighbor sets
//    val setGraph: Graph[Map[VertexId, (Int, Int)], (Int, Int)] = graph.outerJoinVertices(nbrSets) {
//      (_, _, optSet) => optSet.getOrElse(Map.empty)
//    }
//
//    def intersectIntervals(edge1TimeInterval: (Int, Int), queryTimeInterval: (Int, Int)): Option[(Int, Int)] = {
//      val start = Math.max(edge1TimeInterval._1, queryTimeInterval._1)
//      val end = Math.min(edge1TimeInterval._2, queryTimeInterval._2)
//      if (start <= end) {
//        Some((start, end))
//      } else {
//        None
//      }
//    }
//
//    def edgeFunc(
//                  ctx: EdgeContext[Map[VertexId, (Int, Int)], (Int, Int), (Float, Float, Float)],
//                  queryTimeIntervalLength: Int
//                ): Unit = {
//
//      val (smallSet, largeSet) = if (ctx.srcAttr.size <= ctx.dstAttr.size) {
//        (ctx.srcAttr, ctx.dstAttr)
//      } else {
//        (ctx.dstAttr, ctx.srcAttr)
//      }
//
//      val allKeys = smallSet.keySet ++ largeSet.keySet
//      val commonKeys = smallSet.keySet.intersect(largeSet.keySet)
//      val commonKeysSize = commonKeys.size
//
//      var Total: Float = 0f
//      var Total2: Float = 0f
//      var Total1: Float = 0f
//      var score: Float = 0f
//      var sumTimeIntervalLengths: Float = 0f
//
//      //val srcToDstInterval = ctx.srcAttr.get(ctx.dstId)
//
//      for (commonVertex <- allKeys) {
//
//        val enaOpt = ctx.srcAttr.get(commonVertex)
//        val dyoOpt = ctx.dstAttr.get(commonVertex)
//
//        if (enaOpt.isDefined && ctx.dstId != commonVertex) {
//          val interSrcCommonOpt = intersectIntervals(enaOpt.get, ctx.srcAttr(ctx.dstId))
//
//          if (dyoOpt.isDefined && interSrcCommonOpt.isDefined && ctx.srcId != commonVertex) {
//            val finalIntersectOpt = intersectIntervals(interSrcCommonOpt.get, dyoOpt.get)
//
//            if (finalIntersectOpt.isDefined) {
//              val length = finalIntersectOpt.get._2 - finalIntersectOpt.get._1 + 1
//              val triangleScore = length / queryTimeIntervalLength.toFloat
//              val norm = triangleScore / 2f
//              score += norm
//              Total1 += norm
//              if (commonKeysSize > 0) sumTimeIntervalLengths += triangleScore / commonKeysSize
//            } else {
//              val len = interSrcCommonOpt.get._2 - interSrcCommonOpt.get._1 + 1
//              Total2 += len / (2f * queryTimeIntervalLength)
//            }
//
//          } else if (dyoOpt.isDefined && ctx.srcId != commonVertex) {
//            val inter = intersectIntervals(dyoOpt.get, ctx.srcAttr(ctx.dstId))
//            if (inter.isDefined) {
//              val len = inter.get._2 - inter.get._1 + 1
//              Total += len / (2f * queryTimeIntervalLength)
//            }
//
//          } else if (interSrcCommonOpt.isDefined) {
//            val len = interSrcCommonOpt.get._2 - interSrcCommonOpt.get._1 + 1
//            Total2 += len / (2f * queryTimeIntervalLength)
//          }
//
//        } else if (dyoOpt.isDefined && ctx.srcId != commonVertex) {
//          val inter = intersectIntervals(dyoOpt.get, ctx.srcAttr(ctx.dstId))
//          if (inter.isDefined) {
//            val len = inter.get._2 - inter.get._1 + 1
//            Total += len / (2f * queryTimeIntervalLength)
//          }
//        }
//      }
//
//      ctx.sendToSrc((sumTimeIntervalLengths, score, Total1 + Total2))
//      ctx.sendToDst((sumTimeIntervalLengths, score, Total1 + Total))
//    }
//
//    // Aggregate both triangle scores and collected time interval lengths
//    val scoresAndIntervalLengths: VertexRDD[(Float, Float, Float)] = setGraph.aggregateMessages[(Float, Float, Float)](
//      ctx => edgeFunc(ctx, queryTimeIntervalLength),
//      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
//    )
//
//    scoresAndIntervalLengths
//  }
//}

package communityDetection

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
//import scala.reflect.ClassTag

class TriangleScoringWithTimeIntervals extends Serializable {

  def run(queryTimeIntervalLength: Int, graph: Graph[Int, (Int, Int)]): VertexRDD[(Float, Float, Float)] = {
    runPreProcessed(graph, queryTimeIntervalLength)
  }

  private def runPreProcessed(graph: Graph[Int, (Int, Int)], queryTimeIntervalLength: Int): VertexRDD[(Float, Float, Float)] = {
    val nbrSets: VertexRDD[Array[(VertexId, (Int, Int))]] = graph.aggregateMessages[Array[(VertexId, (Int, Int))]](
      triplet => {
        triplet.sendToSrc(Array((triplet.dstId, triplet.attr)))
        triplet.sendToDst(Array((triplet.srcId, triplet.attr)))
      },
      (a, b) => a ++ b
    )

    val setGraphUnpersisted = graph.outerJoinVertices(nbrSets) {
      (_, _, optSet) => optSet.getOrElse(Array.empty)
    }

    val setGraph = {
      val g = setGraphUnpersisted
      // Persist only if not already persisted
      if (!g.vertices.getStorageLevel.isValid || !g.edges.getStorageLevel.isValid) {
        g.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      g
    }


    def intersectIntervals(a: (Int, Int), b: (Int, Int)): Option[(Int, Int)] = {
      val start = math.max(a._1, b._1)
      val end = math.min(a._2, b._2)
      if (start <= end) Some((start, end)) else None
    }

    def edgeFunc(ctx: EdgeContext[Array[(VertexId, (Int, Int))], (Int, Int), (Float, Float, Float)]): Unit = {
      val srcMap = ctx.srcAttr.toMap
      val dstMap = ctx.dstAttr.toMap

      val (smallMap, largeMap) = if (srcMap.size <= dstMap.size) (srcMap, dstMap) else (dstMap, srcMap)
      val commonKeys = smallMap.keySet.intersect(largeMap.keySet)
      val commonKeysSize = commonKeys.size

      var total, total1, total2, score, sumTimeIntervalLengths = 0f

      for (commonVertex <- smallMap.keys ++ largeMap.keys) {
        val enaOpt = srcMap.get(commonVertex)
        val dyoOpt = dstMap.get(commonVertex)

        if (enaOpt.isDefined && ctx.dstId != commonVertex) {
          val interSrcCommonOpt = intersectIntervals(enaOpt.get, srcMap.getOrElse(ctx.dstId, (Int.MinValue, Int.MinValue)))

          if (dyoOpt.isDefined && interSrcCommonOpt.isDefined && ctx.srcId != commonVertex) {
            val finalIntersectOpt = intersectIntervals(interSrcCommonOpt.get, dyoOpt.get)
            if (finalIntersectOpt.isDefined) {
              val length = finalIntersectOpt.get._2 - finalIntersectOpt.get._1 + 1
              val triangleScore = length / queryTimeIntervalLength.toFloat
              val norm = triangleScore / 2f
              score += norm
              total1 += norm
              if (commonKeysSize > 0) sumTimeIntervalLengths += triangleScore / commonKeysSize
            } else {
              val len = interSrcCommonOpt.get._2 - interSrcCommonOpt.get._1 + 1
              total2 += len / (2f * queryTimeIntervalLength)
            }
          } else if (dyoOpt.isDefined && ctx.srcId != commonVertex) {
            val inter = intersectIntervals(dyoOpt.get, srcMap.getOrElse(ctx.dstId, (Int.MinValue, Int.MinValue)))
            if (inter.isDefined) {
              val len = inter.get._2 - inter.get._1 + 1
              total += len / (2f * queryTimeIntervalLength)
            }
          } else if (interSrcCommonOpt.isDefined) {
            val len = interSrcCommonOpt.get._2 - interSrcCommonOpt.get._1 + 1
            total2 += len / (2f * queryTimeIntervalLength)
          }
        } else if (dyoOpt.isDefined && ctx.srcId != commonVertex) {
          val inter = intersectIntervals(dyoOpt.get, srcMap.getOrElse(ctx.dstId, (Int.MinValue, Int.MinValue)))
          if (inter.isDefined) {
            val len = inter.get._2 - inter.get._1 + 1
            total += len / (2f * queryTimeIntervalLength)
          }
        }
      }

      ctx.sendToSrc((sumTimeIntervalLengths, score, total1 + total2))
      ctx.sendToDst((sumTimeIntervalLengths, score, total1 + total))
    }

    val scoresAndIntervalLengths: VertexRDD[(Float, Float, Float)] = setGraph.aggregateMessages[(Float, Float, Float)](
      ctx => edgeFunc(ctx),
      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    )

    scoresAndIntervalLengths
  }
}



