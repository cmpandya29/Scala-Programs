import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
    val ip = sc.textFile(args(0))

    def readgraph(row:Array[String]): (Long,Long,List[Long])={

      (row(0).toLong,row(0).toLong,row.tail.map(_.toString.toLong).toList)
    }

    var graph = ip.map(line => { readgraph(line.split(","))  })
    val oldGraph = graph.map( line => (line._1,line))

    for(i <- 1 to 5){

      graph = graph.flatMap(
        map => map match{ case (a, b, list) => (a, b) :: list.map(x => (x, b) ) } )
        .reduceByKey((a, b) => math.min(a,b))
        .join(oldGraph)
        .map(a => (a._2._2._2, a._2._1, a._2._2._3))

    }
    val graphFinal = graph.map(node => (node._2, 1))
      .reduceByKey(_+_).sortByKey().map {case (a,b) => a+" "+b}

    graphFinal.saveAsTextFile("output-distr")
  }

}
