package wcc

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import wcc.GraphXOps.GXOperations
import org.apache.spark.graphx.{Edge, Graph}

/**
 * Adds the SCD algorithm to the GraphFrame class.
 * For documentation refer to [[wcc.DistributedWCC]].
 */
object GraphFrameOps {
  implicit class GFOperations(graph: GraphFrame) {

    def scalableCommunityDetection(spark: SparkSession, logFilePath: String, queryTimeInterval: (Int, Int), queryTimeIntervalLength: Int): DataFrame = {

      // Convert GraphFrame to DataFrame
      val df = graph.edges.select("src", "dst", "attr1", "attr2")

      // Convert DataFrame to RDD[Edge]
      val edgesRDD = df.rdd.map {
        case Row(src: Int, dst: Int, attr1: Int, attr2: Int) =>
          Edge(src, dst, (attr1, attr2))
      }

      // Create RDD of vertices
      val verticesRDD = edgesRDD.flatMap(edge => Iterable(edge.srcId, edge.dstId)).distinct().map(id => (id, id))

      // Create GraphX graph
      val gx = Graph(verticesRDD, edgesRDD)
      gx
      // Perform scalable community detection (assuming scalableCommunityDetection is defined in GXOperations)
      val resRDD = gx.scalableCommunityDetection(spark.sparkContext, logFilePath, queryTimeInterval: (Int, Int), queryTimeIntervalLength)

      // Convert result RDD to DataFrame
      spark.createDataFrame(resRDD, StructType(Seq(
        StructField(name = "id", dataType = IntegerType, nullable = false),
        StructField(name = "cId", dataType = IntegerType, nullable = false)
      )))
    }
  }
}
