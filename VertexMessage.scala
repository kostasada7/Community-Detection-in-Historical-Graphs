package wcc

import org.apache.spark.graphx._

class VertexMessage extends Serializable {
  var vId: VertexId = -1L
  var vt: Double = 0f
  var t1: Double = 0f
  var cId: VertexId = -1L
  var cc: Double = 0f

  // Constructor
  def this(vId: VertexId, t: Float, vt: Float, t1: Float, cId: VertexId) {
    this()
    this.vId = vId
    this.cId = cId
    this.vt = vt
    this.cc = if (t1 != 0) {
      t / t1
    } else {
      0.0 // Handle case where vt < 2 to avoid invalid combinatorial calculation
    }
  }

  def isCenter: Boolean = {
    this.vId == this.cId
  }
}

object VertexMessage {

  def create(vertexData: VertexData): VertexMessage = {
    new VertexMessage(vertexData.vId, vertexData.t, vertexData.vt, vertexData.t1, vertexData.cId)
  }
  // Added: initial dummy VertexMessage for Pregel initialMsg
  def initial(): VertexMessage = new VertexMessage(-1L, 0f, 0f, 0f, -1L)

  implicit val ordering: Ordering[VertexMessage] = Ordering.by { data: VertexMessage =>
    (data.cc, data.vt, data.vId)
  }
}
