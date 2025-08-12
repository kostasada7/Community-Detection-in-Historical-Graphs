package communityDetection

import scala.reflect.ClassTag
import org.apache.spark.graphx._

class TriangleScoringWithTimeIntervals {

  def run[VD: ClassTag](graph: Graph[VD, Float]): VertexRDD[(Float, Float, Float)] = {

    // Step 1: Build neighbor sets with edge weights
    val nbrSets: VertexRDD[Map[VertexId, Float]] = graph.aggregateMessages[Map[VertexId, Float]](
      triplet => {
        triplet.sendToSrc(Map(triplet.dstId -> triplet.attr))
        triplet.sendToDst(Map(triplet.srcId -> triplet.attr))
      },
      (a, b) => a ++ b
    )

    // Step 2: Attach neighbor sets to each vertex
    val setGraph = graph.outerJoinVertices(nbrSets)((_, _, opt) => opt.getOrElse(Map.empty))

    // Step 3: Edge function
    def edgeFunc(ctx: EdgeContext[Map[VertexId, Float], Float, (Float, Float, Float)]): Unit = {
      val (smaller, larger) =
        if (ctx.srcAttr.size < ctx.dstAttr.size) (ctx.srcAttr, ctx.dstAttr)
        else (ctx.dstAttr, ctx.srcAttr)

      val allNeighbors = (smaller.keySet ++ larger.keySet)
      val commonNeighbors = smaller.keySet.intersect(larger.keySet) - ctx.srcId - ctx.dstId

      val commonNeighborsSize = commonNeighbors.size
      var sumWeightContributions = 0f
      var triangleScore = 0f
      var totalScore = 0f
      var totalScore1 = 0f
      var totalScore2 = 0f
      //print("123")
      for (nbr <- allNeighbors) {
        val w1 = ctx.attr

        if (commonNeighbors.contains(nbr)) {
          val w2 = smaller(nbr)
          val w3 = larger(nbr)

          val triScore = (w1 + w2 + w3) / 3f
          triangleScore += triScore
          sumWeightContributions += triScore/commonNeighborsSize
          totalScore += triScore/2

        } else if (larger.keySet.contains(nbr) && larger.keySet.contains(ctx.srcId) && ctx.srcId != nbr) {
          //println(larger.keySet, smaller.keySet, nbr, ctx.srcId, ctx.dstId)
          val w3 = larger(nbr)
          val tritScore = (w1 + 1f + w3) / 3f  // Semi-triangle logic
          totalScore1 += tritScore/2f

        } else if (smaller.keySet.contains(nbr) && smaller.keySet.contains(ctx.srcId) && ctx.srcId != nbr){
          val w2 = smaller(nbr)

          val altScore = (w1 + w2 + 1f) / 3f  // Fallback logic
          totalScore1 += altScore/2f
        }else if (larger.keySet.contains(nbr) && larger.keySet.contains(ctx.dstId) && ctx.dstId != nbr) {
          val w3 = larger(nbr)
          val tritScore1 = (w1 + 1f + w3) / 3f  // Semi-triangle logic
          totalScore2 += tritScore1/2f

        } else if (smaller.keySet.contains(nbr) && smaller.keySet.contains(ctx.dstId) && ctx.dstId != nbr){
          val w2 = smaller(nbr)

          val altScore1 = (w1 + w2 + 1f) / 3f  // Fallback logic
          totalScore2 += altScore1/2
        }
      }

      ctx.sendToSrc((sumWeightContributions, triangleScore / 2f, totalScore + totalScore2))
      ctx.sendToDst((sumWeightContributions, triangleScore / 2f, totalScore + totalScore1))
    }

    // Step 4: Aggregate results using edgeFunc
    val scoresAndIntervalLengths: VertexRDD[(Float, Float, Float)] = setGraph.aggregateMessages[(Float, Float, Float)](
      edgeFunc,
      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    )

    scoresAndIntervalLengths
  }
}

//package communityDetection
//
//import scala.reflect.ClassTag
//import org.apache.spark.graphx._
//
//class TriangleScoringWithTimeIntervals {
//
//  def run[VD: ClassTag](graph: Graph[VD, Double]): VertexRDD[(Double, Double, Double)] = {
//
//    // Step 1: Create neighbor map with weights
//    val neighborMap: VertexRDD[Map[VertexId, Double]] = graph.aggregateMessages[Map[VertexId, Double]](
//      ctx => {
//        ctx.sendToSrc(Map(ctx.dstId -> ctx.attr))
//        ctx.sendToDst(Map(ctx.srcId -> ctx.attr))
//      },
//      (a, b) => a ++ b
//    )
//
//    // Step 2: Attach neighbors to graph
//    val setGraph = graph.outerJoinVertices(neighborMap) {
//      case (_, _, Some(nbrs)) => nbrs
//      case (_, _, None) => Map.empty[VertexId, Double]
//    }
//
//    // Step 3: Optimized edgeFunc
//    def edgeFunc(ctx: EdgeContext[Map[VertexId, Double], Double, (Double, Double, Double)]): Unit = {
//      val srcNbrs = ctx.srcAttr
//      val dstNbrs = ctx.dstAttr
//
//      val (aMap, bMap) = if (srcNbrs.size < dstNbrs.size) (srcNbrs, dstNbrs) else (dstNbrs, srcNbrs)
//
//      val w1 = ctx.attr
//      val common = aMap.keySet.intersect(bMap.keySet) - ctx.srcId - ctx.dstId
//
//      val commonSize = common.size.toDouble.max(1.0)  // avoid division by 0
//      var triangleScoreSum = 0.0
//      var weightedAvgSum = 0.0
//      var partialDst = 0.0
//      var partialSrc = 0.0
//
//      // Full triangle score
//      common.foreach { nbr =>
//        val w2 = aMap(nbr)
//        val w3 = bMap(nbr)
//        val triScore = (w1 + w2 + w3) / 3.0
//        triangleScoreSum += triScore
//        weightedAvgSum += triScore / commonSize
//      }
//
//      // Semi-triangles for dst side
//      dstNbrs.foreach { case (nbr, w3) =>
//        if (nbr != ctx.srcId && !common.contains(nbr)) {
//          partialDst += (w1 + 1.0 + w3) / 6.0
//        }
//      }
//
//      // Semi-triangles for src side
//      srcNbrs.foreach { case (nbr, w2) =>
//        if (nbr != ctx.dstId && !common.contains(nbr)) {
//          partialSrc += (w1 + w2 + 1.0) / 6.0
//        }
//      }
//
//      val halfTri = triangleScoreSum / 2.0
//      ctx.sendToSrc((weightedAvgSum, halfTri, halfTri + partialDst))
//      ctx.sendToDst((weightedAvgSum, halfTri, halfTri + partialSrc))
//    }
//
//    // Step 4: Aggregate the results
//    setGraph.aggregateMessages[(Double, Double, Double)](
//      edgeFunc,
//      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
//    )
//  }
//}


