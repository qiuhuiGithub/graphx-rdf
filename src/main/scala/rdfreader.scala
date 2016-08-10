import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * @author szednik
  */

object rdfreader {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("rdfreader"))

  val rdf_type_str = "<" + RDF.`type`.getURI + ">"

  def merge_graphs(graph1: Graph[String, String], graph2: Graph[String, String]): Graph[String, String] = {
    Graph(graph1.vertices.union(graph2.vertices),
      graph1.edges.union(graph2.edges)
    ).partitionBy(RandomVertexCut).
      groupEdges((attr1, attr2) => attr1 + attr2)
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

  def sendDomainMsg(ec: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]]): Unit = {
    if (ec.attr == "<" + RDFS.domain.getURI + ">") {
      ec.sendToSrc(ec.dstAttr._2.getOrElse("domain", Set.empty) + ec.dstId)
    }
  }

  def sendRangeMsg(ec: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]]): Unit = {
    if (ec.attr == "<" + RDFS.range.getURI + ">") {
      ec.sendToSrc(ec.dstAttr._2.getOrElse("range", Set.empty) + ec.dstId)
    }
  }

  def mergeVertexSet(a: Set[VertexId], b: Set[VertexId]): Set[VertexId] = {
    a ++ b
  }

  def graphWithVertexMap(g: Graph[String, String]): Graph[(String, Map[String, Set[VertexId]]), String] = {
    val verts = g.vertices.map(x => (x._1, (x._2, Map.empty: Map[String, Set[VertexId]])))
    Graph(verts, g.edges)
  }

  def updateMapWithSuperclasses(map: Map[String, Set[VertexId]], set: Set[VertexId]): Map[String, Set[VertexId]] = {
    val s2 = map.getOrElse("superclass", Set.empty) ++ set
    map + ("superclass" -> s2)
  }

  def updateMapWithDomainSet(map: Map[String, Set[VertexId]], set: Set[VertexId]): Map[String, Set[VertexId]] = {
    val s2 = map.getOrElse("domain", Set.empty) ++ set
    map + ("domain" -> s2)
  }

  def updateMapWithRangeSet(map: Map[String, Set[VertexId]], set: Set[VertexId]): Map[String, Set[VertexId]] = {
    val s2 = map.getOrElse("range", Set.empty) ++ set
    map + ("range" -> s2)
  }

  def sendDomainOfPropertyMsg(ec: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]]): Unit = {
    if (ec.attr == "uses" && ec.dstAttr != null) {
      ec.sendToSrc(ec.dstAttr._2.getOrElse("domain", Set.empty))
    }
  }

  def sendRangeOfPropertyMsg(ec: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]]): Unit = {
    if(ec.attr == "referenced_by" && ec.dstAttr != null) {
      ec.sendToSrc(ec.dstAttr._2.getOrElse("range", Set.empty))
    }
  }

  def inferTypeViaRange(g: Graph[(String, Map[String, Set[VertexId]]), String]): RDD[Edge[String]] = {

    // generate updated graph with "referenced by" edges
    val referenced_by_edges = g.edges.map(e => Edge(e.dstId, uriHash(e.attr), "referenced_by")).cache()
    val g2 = Graph(g.vertices, g.edges.union(referenced_by_edges))

    // update property vertices with "range" sets
    val v2 = g2.aggregateMessages(sendRangeMsg, mergeVertexSet)
      .rightOuterJoin(g2.vertices)
      .filter( v => v._2._2 != null)
      .map(x => (x._1, (x._2._2._1, updateMapWithRangeSet(x._2._2._2, x._2._1.getOrElse(Set.empty)))))
      .cache()

    // generate RDD of new rdf:type edges based on rdfs:range of referenced_by properties
    inferTypeStatements(Graph(v2, g2.edges), sendRangeOfPropertyMsg, mergeVertexSet)
  }

  def inferTypeViaDomain(g: Graph[(String, Map[String, Set[VertexId]]), String]): RDD[Edge[String]] = {

    // generate updated graph with "uses" edges
    val uses_edges = g.edges.map(e => Edge(e.srcId, uriHash(e.attr), "uses")).cache()
    val g2 = Graph(g.vertices, g.edges.union(uses_edges))

    // update property vertices with "domain" sets
    val v2 = g2.aggregateMessages(sendDomainMsg, mergeVertexSet)
      .rightOuterJoin(g2.vertices)
      .filter( v => v._2._2 != null)
      .map(x => (x._1, (x._2._2._1, updateMapWithDomainSet(x._2._2._2, x._2._1.getOrElse(Set.empty)))))
      .cache()

    // generate RDD of new rdf:type edges based on rdfs:domain of used properties
    inferTypeStatements(Graph(v2, g2.edges), sendDomainOfPropertyMsg, mergeVertexSet)
  }

  def inferTypeStatements(g: Graph[(String, Map[String, Set[VertexId]]), String],
                           sendMsg: EdgeContext[(String, Map[String, Set[VertexId]]), String, Set[VertexId]] => Unit,
                           mergeMsg: (Set[VertexId], Set[VertexId]) => Set[VertexId])
  : RDD[Edge[String]] = {
    g.aggregateMessages(sendMsg, mergeMsg)
      .filter(x => x._2.nonEmpty)
      .flatMap(x => x._2.map(y => (x._1, y)))
      .map(e => Edge(e._1, e._2, rdf_type_str))
      .cache
  }

  def inferTypeViaSubClassOf(g: Graph[(String, Map[String, Set[VertexId]]), String]): RDD[Edge[String]] = {
    val g2 = propagateSubClassOfMsgs(g)
    inferTypeStatements(g2, sendTypeMsg, mergeVertexSet)
  }

  def propagateSubClassOfMsgs(g: Graph[(String, Map[String, Set[VertexId]]), String]): Graph[(String, Map[String, Set[VertexId]]), String] = {

    val verts = g.aggregateMessages(sendSubClassOfMsg, mergeVertexSet)
      .rightOuterJoin(g.vertices)
      .map(x => (x._1, (x._2._2._1, updateMapWithSuperclasses(x._2._2._2, x._2._1.getOrElse(Set.empty)))))
      .cache

    val g2 = Graph(verts, g.edges)

    val not_done = g2.vertices.join(g.vertices)
      .map(x => x._2._1._2.get("superclass") != x._2._2._2.get("superclass"))
      .reduce((a: Boolean, b: Boolean) => a || b)

    if (not_done) {
      propagateSubClassOfMsgs(g2)
    } else {
      g
    }
  }

  def reason(g: Graph[String, String]): Graph[String, String] = {
    val g2 = graphWithVertexMap(g)

    // generate RDD of type statements inferred via rdfs:domain
    val new_type_edges_via_domain = inferTypeViaDomain(g2)

    // generate RDD of type statements inferred via rdfs:range
    val new_type_edges_via_range = inferTypeViaRange(g2)

    // generate new graph with type statements inferred via rdfs:domain & rdfs:range
    val g3 = Graph(g2.vertices, g.edges.union(new_type_edges_via_domain).union(new_type_edges_via_range))

    // generate list of type statements inferred via rdfs:subClassOf
    val new_type_edges_via_subclass = inferTypeViaSubClassOf(g3)

    Graph(g.vertices, g3.edges.union(new_type_edges_via_subclass))
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
    val x2 = triple.indexOf(" ", x1 + 1)
    val p = triple.substring(x1 + 1, x2).trim
    val x3 = triple.lastIndexOf(" ")
    val o = triple.substring(x2 + 1, x3).trim
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
    val objectId = if (isLiteral(triple(2))) literalHash(triple(2)) else uriHash(triple(2))
    (Array((subjectId, triple(0)), (objectId, triple(2))), Edge(subjectId, objectId, triple(1)))
  }

  def readNTriples(sc: SparkContext, filename: String): Graph[String, String] = {
    val r = sc.textFile(filename).map(parseNTriple)
    val z = r.map(makeEdge).collect()
    val vertices = sc.makeRDD(z.flatMap(x => x._1))
    val edges = sc.makeRDD(z.map(x => x._2))
    Graph(vertices, edges)
  }

}
