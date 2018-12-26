import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext
import scala.util.control.Breaks._
import java.io._

object Community {

  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("Girvan").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)
    def cmp1(nodes:(VertexId,VertexId)) : (VertexId, VertexId) = {
      if(nodes._1 < nodes._2)
        nodes
      else
        nodes.swap
    }

    def graphModularity(graph: Graph[String, Double],  m:Double, allEdges:RDD[((VertexId,VertexId),Int)]): Double = {

      /*
      This function takes the graph as input,
      output the modularity of the graph
       */
      // find the partitions first
      val connectedGraph = graph.connectedComponents().vertices.map(_.swap).groupByKey().map(_._2)
      // val c1 = graph.connectedComponents().vertices.map(_.swap)
      //
      //     val c3 = graph.connectedComponents().vertices
      //
      //      c3.foreach(println)
      //      //println(c3)
      //      val c2 = graph.connectedComponents().vertices//.map(_.swap).groupByKey()
      //
      //      c1.foreach(println)
      //      c2.foreach(println)
      //      connectedGraph.foreach(println)
      // the number of edges in the graph
      //val m = graph.numEdges.toDouble

      // get the degrees of all the vertex
      val degrees: RDD[(VertexId, Int)] = graph.degrees

      //val allEdges = graph.edges.map(edge => ((edge.srcId, edge.dstId),1))

      val modularity = connectedGraph
        .map(_.toList.combinations(2).toList).flatMap(line => line).map(line => (line(0), line(1))) // (v1, v2)//find all the possible edges
        .join(degrees) // (u, (v, du))
        .map(line => (line._2._1, (line._1, line._2._2))) // (v, (u, du))
        .join(degrees) // (v, ((u, du), dv))
        .map(line => (cmp1(line._2._1._1, line._1), (line._2._1._2, line._2._2))) // ((u, v), (du, dv))
        .leftOuterJoin(allEdges) // ((u,v), ((du,dv),1 or 0(None)))
        .map(line => (line._2._1, if(line._2._2.nonEmpty) 1.0 else 0.0))//Aij
        .map(line => line._2 - line._1._1*line._1._2/2.0/m).collect().sum//summation over edges in subgraph

      modularity / 2.0 / m

    }
    //call betweenness class to get the graph and the betweenness
    // val betweennessOutput = test.betweennessCalc(args)
    val betweennessOutput = Betweenness.createGraph(args,sc)
    var graph = betweennessOutput._1
    val allBetweenness = betweennessOutput._2
    val m = betweennessOutput._1.numEdges.toDouble
    val graphEdges = graph.edges.map(edge => ((edge.srcId, edge.dstId),1))
    ///descing order betweeness sort
    var descBetweenness = allBetweenness
      .map(line => ((line._1, line._2), line._3))
      .sortBy(-_._2)
      .collect()
    //descBetweenness.foreach(println)
    def maxCount(begin:Int, inc:Int): Int ={

      var count = begin
      var newGraph = graph
      var newDescBetweenness = descBetweenness
      //-----------------remove Edges--------------------------------------
      if (count != 0) {
        val maxBetweenness = newDescBetweenness.take(count).map(_._1).toSet
        newDescBetweenness = newDescBetweenness.drop(count)
        newGraph = newGraph.subgraph(line => !maxBetweenness.contains(cmp1(line.srcId, line.dstId)))

      }
      // calculate the modularity
      var modularity = graphModularity(newGraph,m,graphEdges)

      // println("------------------------ \nmodularity before removal is " + modularity + "\n------------------------")

      // loop until the modularity starts decreasing
      breakable(
        while (true) {
          count += inc
          val maxBetweenness = newDescBetweenness.take(inc).map(_._1).toSet
          newDescBetweenness = newDescBetweenness.drop(inc)
          newGraph = newGraph.subgraph(line => !maxBetweenness.contains(cmp1(line.srcId, line.dstId)))

          // calculate the new modularity
          val newModularity = graphModularity(newGraph,m,graphEdges)
          // println("------------------------\nRemoved " + count + " edge;\nNew modularity is " + newModularity + "\n------------------------")

          //modularity = newModularity
          if (newModularity >= modularity) {
            modularity = newModularity
          } else break
        }
      )

      count
    }

    var begin = 0
    var inc = 10//40,20(Best),10(Best)(Best)--------Tried---------------------
    var lastCount = maxCount(begin, inc)
    //println(lastCount)
    val div = 2
    ///---------------------------BACKTRACKINGGGGGG--------------------------------------------------------------------
    // inc number of edges to be removed in each iterations. Until the unique community with the highest modularity is found.
    breakable(
      while (true) {
        // before modularity decreased
        /// First Half of the graph
        val edges1 = descBetweenness.take(lastCount - inc).map(_._1).toSet
        val firstGraph = graph.subgraph(line => !edges1.contains(cmp1(line.srcId, line.dstId)))
        val newCommunities = firstGraph.connectedComponents().vertices.map(_._2).distinct().count().toInt

        // step at which modularity decreased
        //Remaining part
        val edges2 = descBetweenness.take(lastCount).map(_._1).toSet
        val secondGraph = graph.subgraph(line => !edges2.contains(cmp1(line.srcId, line.dstId)))
        val remainingCommunities = secondGraph.connectedComponents().vertices.map(_._2).distinct().count().toInt

        // check the number of communities decreased only by 1 or doesn't change at all then split the community
        if (remainingCommunities - newCommunities <= 1) {
          val finalCommunities = firstGraph.connectedComponents().vertices.map(_.swap).groupByKey().map(_._2.toArray.sorted).sortBy(_.head)
            .map(line => "["+line.mkString(",")+"]")
          new PrintWriter(args(1) + "Pallavi_Taneja_Community.txt")
          { write(finalCommunities.
            coalesce(1).collect().
            mkString("\n"));
            close
          }

          break
        }
        else {
          begin = lastCount - inc
          inc = inc / div
          lastCount = maxCount(begin, inc)
        }
      }
    )
    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time) / 1000 + " secs")

  }
}

