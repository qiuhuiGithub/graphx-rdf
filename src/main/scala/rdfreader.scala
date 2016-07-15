import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.util.Random

/**
  * @author szednik
  */

object rdfreader {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("rdfreader"))

  def main(args: Array[String]) {

    val input = "data/prov-example.nt"
    val rdf = readNTriples(sc, input)

    rdf.triplets
      .collect()
      .foreach(println)

    sc.stop
  }

  def parseNTriple(triple: String): Array[String] = {
    val x1 = triple.indexOf(" ")
    val s = triple.substring(0, x1).trim
    val x2 = triple.indexOf(" ", x1+1)
    val p = triple.substring(x1+1, x2).trim
    val x3 = triple.lastIndexOf(" ")
    val o = triple.substring(x2+1, x3).trim
    Array(s, p, o)
  }

  def uriHash(uri: String): VertexId = {
    uri.toLowerCase.hashCode.toLong
  }

  // there is ~probably~ a very small chance of collision with literal IDs
  def literalHash(literal: String): VertexId = {
    Random.nextLong()
  }

  def isLiteral(value: String): Boolean = {
    value.charAt(0) match {
      case '"' => true
      case '<' => false
      case '_' => false
      case _ => true
    }
  }

  def makeEdge(triple: Array[String]): (Array[(VertexId, String)], Edge[String]) = {
    val subjectId = uriHash(triple(0))
    val objectId = if(isLiteral(triple(2))) literalHash(triple(2)) else uriHash(triple(2))
    (Array((subjectId, triple(0)), (objectId, triple(2))) , Edge(subjectId, objectId, triple(1)))
  }

  def readNTriples(sc: SparkContext, filename:String): Graph[String, String] = {
    val r = sc.textFile(filename).map(parseNTriple)
    val z = r.map(makeEdge).collect()
    val vertices = sc.makeRDD(z.flatMap(x => x._1))
    val edges = sc.makeRDD(z.map(x => x._2))
    Graph(vertices, edges)
  }

}
