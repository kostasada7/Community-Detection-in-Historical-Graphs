package wcc

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import wcc.GraphXOps.GXOperations
import org.apache.spark.graphx.{Edge, Graph}

object GraphFrameOps {

  implicit class GFOperations(graph: GraphFrame) {

    def scalableCommunityDetection(spark: SparkSession, logFilePath:String): DataFrame = {

      //import spark.implicits._

      // Convert GraphFrame to DataFrame
      val df = graph.edges.select("src", "dst", "normalizedOverlap")

      // Debug print (optional)
      df.show()

      // Convert DataFrame to RDD[Edge[Long]]
      val edgesRDD = df.rdd.map { row =>
        val src = row.getAs[Any]("src").toString.toLong
        val dst = row.getAs[Any]("dst").toString.toLong
        val attr = row.getAs[Any]("normalizedOverlap").toString.toDouble
        Edge(src, dst, attr)
      }

      // Create RDD of vertices
      val verticesRDD = edgesRDD.flatMap(edge => Seq(edge.srcId, edge.dstId)).distinct().map(id => (id, id))

      // Create GraphX graph
      val gx = Graph(verticesRDD, edgesRDD)  // edgesRDD is now Edge[Double]

      // Call your custom community detection method
      val resRDD = gx.scalableCommunityDetection(spark.sparkContext,logFilePath)

      // Convert result RDD to DataFrame
      spark.createDataFrame(resRDD, StructType(Seq(
        StructField(name = "id", dataType = LongType, nullable = false),
        StructField(name = "cId", dataType = LongType, nullable = false)
      )))
    }
  }
}
