package com.lietou.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Description : some words
 * Author: chris 
 * Date: 2014/9/12
 */
object GraphxCore {

  def main(args: Array[String]) {
    // spark://name1:7077
    val sc = new SparkContext("local", "GraphxCore", System.getenv("SPARK_HOME"))

    // 创建顶点RDD,vertex(VertexId, (String, String))
    val users: RDD[(VertexId, (String, String))] = {
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))
    }

    // 创建边RDD,Edge(srcid,dstid,attr)
    val relations: RDD[Edge[String]] = {
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 9L, "Stranger"),// 并不存在9这个顶点
        Edge(5L, 7L, "pi")
      ))

    }

    // 其主要作用就在于当如果想描述一种ralationships中不存在的目标顶点的时候就会使用这个defaultUser，
    val defaultUser = ("John Doe", "Missing") // 不存在的顶点的属性，例如顶点5连接到一个不存在的顶点9时，顶点9的属性就是这个defaultUser

    // 构建图
    val graph = Graph(users, relations, defaultUser)

    //val graph: Graph[(String, String), String] // Constructed from above

    // 统计顶点属性是postdoc的顶点个数
    val vnum = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}.count
    println("vertices.attr._1 = postdoc count's " + vnum)

    // 统计边源顶点ID大于目标顶点ID的边的数量
    //num = graph.edges.filter(e => e.srcId > e.dstId).count
    val enum = graph.edges.filter { case Edge(src, dst, prop) => src > dst}.count
    println("edges.srcId > edges.dstId count's " + enum)

    println("edgs's count ")
    graph.edges.collect.foreach(println)
    /*
    Edge(2,5,colleague)
    Edge(3,7,collab)
    Edge(5,3,advisor)
    Edge(5,7,pi)
     */
    println("vertices's count")
    graph.vertices.collect.foreach(println)
    /*
    (3,(rxin,student))
    (7,(jgonzal,postdoc))
    (9,(John Doe,Missing)) // 注意这里
    (5,(franklin,prof))
    (2,(istoica,prof))

    (9,(John Doe,Missing)) // 注意这里
     */

    //EdgeTriplet其实就是Edge合并Vertex, 实现为 继承Edge并添加了srcattr,dstattr
    println("triplets's count ")
    graph.triplets.foreach(println)
    /*
    ((2,(istoica,prof)),(5,(franklin,prof)),colleague)
    ((3,(rxin,student)),(7,(jgonzal,postdoc)),collab)
    ((5,(franklin,prof)),(3,(rxin,student)),advisor)
    ((5,(franklin,prof)),(7,(jgonzal,postdoc)),pi)

    ((5,(franklin,prof)),(9,(John Doe,Missing)),Stranger)
   */
    val facts: RDD[String] = graph.triplets.map {
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1}
    facts.collect.foreach(println(_))
    /*
      istoica is the colleague of franklin
      rxin is the collab of jgonzal
      franklin is the advisor of rxin
      franklin is the pi of jgonzal
      franklin is the Stranger of John Doe
     */

    // 删除顶点属性为Missing的顶点
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    validGraph.vertices.collect.foreach(println(_))
    /*
    (3,(rxin,student))
    (7,(jgonzal,postdoc))
    (5,(franklin,prof))
    (2,(istoica,prof))
     */
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    /*
    istoica is the colleague of franklin
    rxin is the collab of jgonzal
    franklin is the advisor of rxin
    franklin is the pi of jgonzal
     */

//    val uniqueCosts: VertexRDD[Double] =  graph.vertices.aggregateUsingIndex(nonUnique, (a,b) => a + b)
//    val joinedGraph = graph.joinVertices(uniqueCosts, (id: VertexId, oldCost: Double, extraCost: Double) => oldCost + extraCost)
  }

}
