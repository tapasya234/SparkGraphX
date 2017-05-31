// Databricks notebook source exported at Sat, 5 Nov 2016 00:55:03 UTC
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
// import classes required for using GraphX
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

// schema for co-authors
// fromNode - author1
// toNode - author2
case class Authors(author1: Long, author2: Long)

// parse function for Authors class
def parseAuthors(str: String): Authors = {
  val line = str.split("\t")
  Authors(line(0).toLong, line(1).toLong)
}

//loading the data from the text file. 
val fileRDD = sc.textFile("/FileStore/tables/iccu94t41478300512811/CA_HepTh-9cc05.txt")
val authorsRDD = fileRDD.map(parseAuthors).cache()
//authorsRDD.take(20).foreach(println)

// mapping the vertices
val verticesRDD = authorsRDD.distinct.map(author => (author.author1,"") )
//mapping the vertices
//val edgesRDD = authorsRDD.map(author => ((author.author1, author.author2), 1)).distinct

val edgeRDD = authorsRDD.map{
  author => Edge(author.author1, author.author2, 1L)
}

// Default vertex
val root = "root"
val graph = Graph(verticesRDD, edgeRDD, root)
graph.vertices.take(10)
graph.edges.take(10)

def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if(a._2 > b._2) a else b
}

// Q1. Highest Out Degree
val outDegreeAuthor: (VertexId, Int) = graph.outDegrees.reduce(max)
println("Highest Out Degree: " + outDegreeAuthor)

// Q2. Highest In Degree
val inDegreeAuthor: (VertexId, Int) = graph.inDegrees.reduce(max)
println("Highest In Degree: " + inDegreeAuthor)

// Q3. Page Rank
val ranks = graph.pageRank(0.1).vertices
val temp = ranks.join(verticesRDD)
val temp2 = temp.sortBy(_._2._1, false)
val authorsPageRank = temp2.map(_._2._1)
println("Top 5 pageRank nodes")
authorsPageRank.distinct.take(5).foreach(println)

// Q4. Connected Components
val cc = graph.connectedComponents().vertices
val ccByID = verticesRDD.join(cc).map {
  case(cc) => cc
}

println(ccByID.collect().distinct.mkString("\n"))

//Q5. Triangle Counts
val triCounts = graph.triangleCount().vertices
val triCountsByID = verticesRDD.join(triCounts).map {
  case (id, tc) => tc
}
println(triCountsByID.collect().distinct.mkString("\n"))

// COMMAND ----------


