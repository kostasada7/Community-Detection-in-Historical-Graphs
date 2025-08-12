package wcc

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
 * Created by tariq on 06/01/18.
 */
object GraphXOps {
  implicit class GXOperations[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) {

    def scalableCommunityDetection(sc: SparkContext,logFilePath:String): RDD[Row] = {
      // Step 1: Map vertices to Int
      val intVertices: RDD[(VertexId, Int)] = graph.vertices.map { case (vid, attr) =>
        (vid, vid.toInt) // Example mapping: vertex ID as Int
      }
      val intVertexRDD: VertexRDD[Int] = VertexRDD(intVertices)

      // Step 2: Map edges to (Long, Long)
      val doubleEdges: RDD[Edge[Float]] = graph.edges.map { e =>
        val attrAsFloat = e.attr match {
          case f: Float => f
          case d: Double => d.toFloat
          case other => throw new IllegalArgumentException(s"Unexpected edge attribute type: ${other.getClass}")
        }
        Edge(e.srcId.toInt, e.dstId.toInt, attrAsFloat)
      }



      // Step 3: Create transformed Graph[Int, (Long, Long)]
      val transformedGraph: Graph[Int, Float] = Graph(intVertexRDD, doubleEdges)
      DistributedWCC.run(transformedGraph, sc, logFilePath, isCanonical = true)
        .map{ case(id, cId) => Row.fromSeq(Array(id, cId)) }
    }

    /**
     * Partition the graph in a way that guarantees all vertices connected from
     * a community C1 to a community C2 will all belong to the same partition.
     * @param numCommunities
     * @param cId
     * @return
     */
    def partitionByCommunity(numCommunities: Int, cId: VD => Long): Graph[VD, ED] = {
      val partitionedEdges = graph.triplets.map({e =>
          val partition = math.abs((cId(e.srcAttr), cId(e.dstAttr)).hashCode()) % numCommunities
          (partition, e)
        }).partitionBy(new HashPartitioner(numCommunities))
        .map{ case (p, e) => Edge(e.srcId, e.dstId, e.attr) }
      Graph(graph.vertices, partitionedEdges)
    }
  }
}