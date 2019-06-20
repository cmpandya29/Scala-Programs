import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph => Graph1, VertexId}

object Graph {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("GraphX")
    val sc = new SparkContext(conf)
	
	val ip = sc.textFile(args(0))

    def readgraph(row:Array[String]): (Long,List[Long])={

      (row(0).toLong,row.tail.map(_.toString.toLong).toList)
    }

    val oldGraph = ip.map(line => { readgraph(line.split(","))  }).flatMap(
      map => map match{ case (a, list) => list.map(x => (a, x) ) } )

    val edges: RDD[Edge[String]] =
      oldGraph.map { map => map match{ case (a, b) => Edge(a.toLong, b.toLong) }}

    val graph : Graph1[String, String] = Graph1.fromEdges(edges, "defaultProperty")

    val initialGraph = graph.mapVertices((id, _) => id.toDouble)

    val sssp = initialGraph.pregel(Double.PositiveInfinity, 5)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr != Double.PositiveInfinity) {
         Iterator((triplet.dstId, math.min(triplet.srcAttr, triplet.dstAttr)))
               } else {
                 Iterator.empty
               }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
		 
   var finalOp = sssp.vertices.map(_.swap).groupByKey().map{case (a,b) => (a.toInt,b.toList.size)}.sortByKey().map{case (a,b) => a+" "+b}.coalesce(1)
    for(rec <- finalOp.toLocalIterator) { println(rec)}
  }
}
 
