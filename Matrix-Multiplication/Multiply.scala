import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
	val conf = new SparkConf().setAppName("MatrixMul")
    	val sc = new SparkContext(conf)
    	val M = sc.textFile(args(0)).map( line => { val a = line.split(",")
      			(a(1).toInt,(a(0).toInt,a(2).toDouble)) } )
    	val N = sc.textFile(args(1)).map( line => { val a = line.split(",")
      			(a(0).toInt,(a(1).toInt,a(2).toDouble)) } )
    	val multiplyJoin = M.join(N)
    	val firstMap = multiplyJoin.map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
    	val firstReduce = firstMap.reduceByKey(_ + _).sortByKey()
    	val multiply = firstReduce.map({ case ((i, k), sum) => i+" "+k+" "+sum })
    	multiply.saveAsTextFile(args(2))
  }
}
