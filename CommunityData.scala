package wcc

/**
  * Created by tariq on 01/05/18.
  *
  * Statistics about a certain community
  * @param r the size of the community (number of vertices).
  * @param a number of internal edges.
  * @param b number of external edges.
  */
class CommunityData(val r: Int, val a: Float, val b: Float, val avgCC: Float) extends Serializable {
  // the edge density δ = 2 * number of edges / squared number of vertices.
  //println(a)
  val d: Float = 2 * a / math.pow(r, 2).toFloat

  // Override toString method
  override def toString: String = {
    s"CommunityData(r=$r, a=$a, b=$b, avgCC=$avgCC)"
  }
}