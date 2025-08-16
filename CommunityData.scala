package wcc

/**
  
  * Statistics about a certain community
  * @param r the size of the community (number of vertices).
  * @param a contribution of internal edges.
  * @param b contribution of external edges.
  */
class CommunityData(val r: Int, val a: Float, val b: Float, val avgCC: Float) extends Serializable {
  // the edge density δ = 2 * contribution of edges / squared number of vertices.
  val d: Float = 2 * a / math.pow(r, 2).toFloat

  // Override toString method
  override def toString: String = {
    s"CommunityData(r=$r, a=$a, b=$b, avgCC=$avgCC)"
  }
}
