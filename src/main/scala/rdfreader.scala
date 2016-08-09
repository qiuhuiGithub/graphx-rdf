import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut

import scala.util.Random

/**
  * @author szednik
  */

object rdfreader {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("rdfreader"))

  def merge_graphs(graph1: Graph[String, String], graph2: Graph[String, String]): Graph[String, String] = {
    Graph(graph1.vertices.union(graph2.vertices),
          graph1.edges.union(graph2.edges)
        ).partitionBy(RandomVertexCut).
          groupEdges( (attr1, attr2) => attr1 + attr2 )
  }

  def sendTypeMsg(ec: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]]): Unit = {
    if (ec.attr == "<" + RDF.`type`.getURI + ">") {
      ec.sendToSrc(ec.dstAttr._2.getOrElse("superclass", Set.empty))
    }
  }

  def sendSubClassOfMsg(ec: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]]): Unit = {
    if (ec.attr == "<" + RDFS.subClassOf.getURI + ">") {
      ec.sendToSrc(ec.dstAttr._2.getOrElse("superclass", Set.empty) + ec.dstId)
    }
  }

  def mergeTypeMsg(a: Set[VertexId], b: Set[VertexId]): Set[VertexId] = {
    a ++ b
  }

  def mergeSubClassOfMsg(a: Set[VertexId], b: Set[VertexId]): Set[VertexId] = {
    a ++ b
  }

  def graphWithSubClassOfSet(g: Graph[String, String]): Graph[(String, Map[String, Set[VertexId]]), String] = {
    val verts = g.vertices.map(x => (x._1, (x._2, Map("superclass" -> Set.empty): Map[String, Set[VertexId]])))
    Graph(verts, g.edges)
  }

  def updateMapWithSuperclasses(map: Map[String, Set[VertexId]], set: Set[VertexId]): Map[String, Set[VertexId]] = {
    val s2 = map.getOrElse("superclass", Set.empty) ++ set
    map + ("superclass" -> s2)
  }

  def inferTypeStatements(g: Graph[(String, Map[String, Set[VertexId]]), String]): Array[Edge[String]] = {
    val rdf_type_str = "<"+RDF.`type`.getURI+">"
    g.aggregateMessages(sendTypeMsg, mergeTypeMsg)
      .filter(x => x._2.nonEmpty)
      .flatMap(x => x._2.map(y => (x._1, y)))
      .map(e => Edge(e._1, e._2, rdf_type_str))
      .collect
  }

  def propagateSubClassOfMsgs(g: Graph[(String, Map[String, Set[VertexId]]), String]): Graph[(String, Map[String, Set[VertexId]]), String] = {

    val verts = g.aggregateMessages(sendSubClassOfMsg, mergeSubClassOfMsg)
      .rightOuterJoin(g.vertices)
      .map(x => (x._1, (x._2._2._1, updateMapWithSuperclasses(x._2._2._2, x._2._1.getOrElse(Set.empty)))))
      .collect

    val g2 = Graph(sc.makeRDD(verts), g.edges)

    val not_done = g2.vertices.join(g.vertices)
        .map(x => x._2._1._2.get("superclass") != x._2._2._2.get("superclass"))
      .reduce((a:Boolean, b:Boolean) => a || b)

    if(not_done) {
      propagateSubClassOfMsgs(g2)
    } else {
      g
    }
  }

  def reason(g: Graph[String, String]): Graph[String, String] = {
    val g2 = graphWithSubClassOfSet(g)
    val g3 = propagateSubClassOfMsgs(g2)
    val new_edges = inferTypeStatements(g3)
    Graph(g.vertices, g.edges.union(sc.makeRDD(new_edges)))
  }

  def main(args: Array[String]) {

    val ontology = "data/ex.nt"
    val ontology_rdf = readNTriples(sc, ontology)

    val data = "data/test.nt"
    val data_rdf = readNTriples(sc, data)

    val rdf = merge_graphs(ontology_rdf, data_rdf)

    val inferred_rdf = reason(rdf)
    inferred_rdf.triplets.collect.foreach(t => println(t.srcAttr, t.attr, t.dstAttr))

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
