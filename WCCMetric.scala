package wcc

import org.apache.spark.graphx.{Graph, _}
import scala.reflect.ClassTag

/**
  * Created by tariq on 08/01/18.
  * functions to calculate the Weighted Community Clustering metric
  */
object WCCMetric {

  /**
   * calculates the vertex local WCC in respect to community c
   */
  def computeWccV(vData: VertexData, communityData: CommunityData, vtC: Float, tC: Float): Float = {
    println(s" Node: ${vData.vId} sumTimeIntervalLengths: $vtC, counter: $tC, t: ${vData.t}, vt: ${vData.vt}, t1: ${vData.t1}")
    if (vData.t == 0) return 0f
    val numerator = tC * vData.vt
    val denominator = vData.t * (communityData.r - 1 + vData.vt - vtC)
    numerator / denominator
  }

  //def combinations(n: Int, k: Int): BigInt = {

      //(BigInt(1) to k).foldLeft(BigInt(1)) { (acc, i) =>
       // acc * (n - i + 1) / i
      //}
  //}


  /**
   * Calculates the approximation of the change to the global wcc that
   * would be caused by inserting this vertex into the new community.
   */
  //def computeWccI(vData: VertexData, cData: CommunityData, dIn: Double, dOut: Double, globalCC: Double, q: Double): Double = {
    //if (dIn >= 2) {
      //val numCombinations = combinations(dIn, 2).toDouble
      //val denomCombinations = combinations(dIn + dOut, 2).toDouble
      //val commonFactor = 1 * cData.avgCC * vData.vt
      //val divisor =1 * globalCC * (cData.r - 1 + vData.vt - (dIn * vData.vt / (dIn + dOut)))
      //commonFactor / divisor
    //} else {
    //  0.0
    //}
  //}


  /**
    * Calculates the approximalogCommunities.println("Analytical Communittion of the change to the global wcc that
    * would be caused by inserting this vertex into the new community.
    */
  def computeWccI(cData: CommunityData, dIn: Float, dOut: Float, globalCC: Float, v: Long):Float = {
    val q = (cData.b - dIn) / cData.r
    val t1 = theta1(cData.r, cData.d, dIn, dOut, globalCC, q)
    val t2 = theta2(cData.r, cData.d, globalCC, q)
    val t3 = theta3(cData.r, cData.d, dIn, dOut, globalCC)

    (dIn * t1 + (cData.r - dIn) * t2 + t3) / v

  }

  private def theta1(r: Int, d: Float, dIn: Float, dOut: Float, w: Float, q: Float):Float = {
    val numerator = ((r - 1) * d + 1 + q) * (dIn - 1) * d
    val denominator = (r + q) * ((r - 1) * (r - 2) * math.pow(d, 3) + (dIn - 1) * d + q * (q - 1) * d * w + q * (q - 1) * w + dOut * w)
    (numerator / denominator).toFloat
  }

  private def theta2(r: Int, d: Float, w: Float, q: Float):Float = {
    val numerator = (r - 1) * (r - 2) * math.pow(d, 3) * ((r - 1) * d + q)
    val denominator = ((r - 1) * (r - 2) * math.pow(d, 3) + q * (q - 1) * w + q * (r - 1) * d * w) * (r + q) * (r - 1 + q)
    (- numerator / denominator).toFloat
  }

  private def theta3(r: Int, d: Float, dIn: Float, dOut: Float, w: Float):Float = {
    val numerator = dIn * (dIn - 1) * d * (dIn + dOut)
    val denominator = (dIn * (dIn - 1) * d + dOut * (dOut - 1) * w + dOut * dIn * w) * (r + dOut)
    numerator / denominator
  }

}
