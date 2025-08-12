package wcc

import org.apache.spark.graphx.VertexId

/**
 * Created by tariq on 01/01/18.
 *
 * @param vId the vertex Identifier.
 * @param t the number of links between neighbors of the vertex aka triangle count.
 * @param vt The number of vertices that form at least one triangle with x.
 */

class VertexData(val vId: VertexId = -1L, val t: Float = 0, val vt: Float = 0, val t1: Float) extends Serializable {

  var cId: VertexId = vId

  def cc: Float = {
    // Adjust the combinatorial calculation as per the requirements
    if (t1 != 0) {
      t / t1
    } else {
      0f // Handle case where vt < 2 to avoid invalid combinatorial calculation
    }
  }

  var changed: Boolean = false

  var neighbors: List[VertexMessage] = List.empty

  def copy(): VertexData = {
    val v = new VertexData(this.vId, this.t, this.vt, this.t1)
    v.cId = this.cId
    v.changed = this.changed
    v.neighbors = this.neighbors
    v
  }

  def isCenter: Boolean = {
    this.vId == this.cId
  }

  def compareTo(vs: VertexData): (VertexData, VertexData) = {
    if (VertexData.ordering.lt(this, vs)) {
      (this, vs)
    } else {
      (vs, this)
    }
  }
}

object VertexData {
  implicit val ordering: Ordering[VertexData] = Ordering.by { data: VertexData =>
    (data.cc, data.vt, data.vId)
  }
}

