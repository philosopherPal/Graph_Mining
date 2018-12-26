import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import scala.collection.immutable.Set
import scala.collection.immutable.Map
import scala.collection.immutable.List
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._



object Betweenness{
  def createGraph(args: Array[String],sc:SparkContext): (Graph[String, Double],RDD[(VertexId, VertexId, Double)]) = {
    //,RDD[(VertexId, VertexId, Double)]
    //    val sparkConf = new SparkConf().setAppName("Girvan").setMaster("local[8]")
    //    val sc = new SparkContext(sparkConf)
    var fileRDD = sc.textFile(args(0))
    var header = fileRDD.first()
    fileRDD = fileRDD.filter(x => x != header)
    var userRDD = fileRDD.map(_.split(",")).map(c => ((c(0).toInt), c(1).toInt))
    var users = fileRDD.map(_.split(",")).map(c => ((c(0).toInt), c(1).toInt)).groupByKey().sortByKey()
      .map { case (k, v) => (k, v.toSet) }.collect()
    //users.foreach(println)
    val impUsers = users//.filter(_._2.size >= 7)
    //filtered_users.foreach(println)

    var coratedUsers =Set[(Int, Int)]()

    for (i <- impUsers) {
      for (j <- impUsers) {
        if (i != j && i._1 < j._1) {
          var set = i._2.intersect(j._2)
          if (set.size >= 7) {
            coratedUsers += ((i._1, j._1))
          }
        }
      }
    }
    val coratedUserList = coratedUsers.toList
    //intersect_users.foreach(println)
    //println("size", coratedUsers.size)

    val distinct_users = coratedUserList.flatMap { case (a, b) => List(a, b) }.distinct

    val intersectUserRDD = sc.parallelize(coratedUserList)
    val distinct_user_rdd = sc.parallelize(distinct_users)
    val distinctUsers: RDD[(VertexId)] = distinct_user_rdd.distinct().map(id => id)
    val userNodes: RDD[(VertexId, String)] = distinctUsers.map(line => (line, line.toString))
    println(userNodes.count())
    val userList = userNodes.collect().map(c=>c._1)

    val edges = intersectUserRDD.map({ case (id1, id2) => Edge(id1, id2, 0.0) })
    var graph = Graph(userNodes, edges)
    val adjacencyMatrixMap = graph.collectNeighborIds(EdgeDirection.Either).toLocalIterator.toMap
    //    var neighbours = Map.empty[Long,List[Long]]
    //    for (i<- adjacencyMatrixMap)
    //    {
    //      neighbours += i._1->i._2.toList
    //    }
    //    //println(neighbours)
    // println(adjacencyMatrixMap.size)
    def calculateBetweeness(node: VertexId) :Map[(VertexId,VertexId),Double]={
      val queueUsers = new Queue[VertexId]()
      val stackUsers = new Stack[VertexId]()
      var level:Map[VertexId ,Long]=Map()
      var effectiveEdges:Map[VertexId, Long]=Map()
      var predecessorMap:Map[VertexId, List[VertexId]]=Map()
      var btweeness:Map[(VertexId,VertexId), Double]= Map()
      var credit:Map[VertexId, Double]=Map()
      for(uid <- userList){
        predecessorMap+=(uid->List())
        effectiveEdges+=(uid->0)
        level+=(uid->Int.MaxValue)
        credit+=(uid->0.0)
      }
      queueUsers.enqueue(node)
      level += (node -> 0)
      //Perform BFS, maintain list of predecessors, user levels
      while(!queueUsers.isEmpty){
        val currentNode = queueUsers.dequeue()
        stackUsers.push(currentNode)
        val adj = adjacencyMatrixMap(currentNode)
        for(user <- adj){
          if(level(user)  == Int.MaxValue){
            level += (user ->(level(currentNode) + 1))
            queueUsers.enqueue(user)
          }
          if(level(user) == level(currentNode) + 1){
            effectiveEdges += (user->(effectiveEdges(user) + 1))
            predecessorMap+=(user->(currentNode::predecessorMap(user)))
          }
        }
      }
      while(!stackUsers.isEmpty){
        val popUser = stackUsers.pop()
        credit+=((popUser)->(credit(popUser)+1))
        val l:Long = predecessorMap(popUser).length
        for(user <- predecessorMap(popUser)){
          if(level(user) == level(popUser) - 1){
            btweeness+=((user,popUser)-> credit(popUser)/effectiveEdges(popUser))
            credit += (user->(credit(user) + credit(popUser)/effectiveEdges(popUser)))//effectiveEdges(popUser)
            //            btweeness.put((user,popUser), credit(popUser)/effectiveEdges(popUser))
            //            credit(user) = credit(user) + credit(popUser)/effectiveEdges(popUser)
          }
        }
      }
      //btweeness.foreach(println)
      //println(btweeness)
      //println("HI")
      btweeness
    }
    //def finalcalculation(graph: Graph)
    val sortedBtweeness = userNodes.map(line => calculateBetweeness(line._1))//,userList,adjacencyMatrixMap))
      .flatMap(record => record).map(record => (cmp1(record._1),record._2)).reduceByKey(_+_).sortByKey()//RDD[Map[(VertexId,VertexId),Int]]
    // val temp = userNodes.map(line => calculateBetweeness(line._1)) //.map(record => (cmp1(record._1),record._2)).reduceByKey(_+_).sortByKey()//,userList,adjacencyMatrixMap))
    //.flatMap(record => record)

    //temp.foreach(println)
    val finalBtweeness = sortedBtweeness.map(line => (line._1._1,line._1._2,line._2/2))
    //    var outputList = finalBtweeness.collect().sortWith(cmp).toList
    //    val output = sc.parallelize(outputList)
    //    new PrintWriter(args(1)) { write(output.coalesce(1).collect().mkString("\n")); close}
    // outputList.foreach(println)
    //println(outputList.length)
    //finalBtweeness.coalesce(1).collect().foreach(println)
    // allBetweenness.foreach(println)
    //println(allBetweenness)
    //println(adjacencyMatrixMap)
    //adjacencyMatrixMap.foreach(println)

    (graph,finalBtweeness)
  }

  def cmp1(nodes:(VertexId,VertexId)) : (VertexId, VertexId) = {
    if(nodes._1 < nodes._2)
      nodes
    else
      nodes.swap
  }
  def cmp(rdd1:(VertexId, VertexId, Double), rdd2:(VertexId, VertexId, Double)): Boolean = {
    var res:Boolean = false
    if (rdd1._1 != rdd2._1) {
      res = rdd1._1 < rdd2._1
    } else {
      res = rdd1._2 < rdd2._2
    }

    res
  }

  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("Girvan").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)
    val betweennessOutput = createGraph(args,sc)
    val result = betweennessOutput._2
    val outputList = result.collect().sortWith(cmp).toList
    val output = sc.parallelize(outputList)
    new PrintWriter(args(1) + "Pallavi_Taneja_Betweenness.txt") { write(output.coalesce(1).collect().mkString("\n")); close}

    //createGraph(args,sc)
    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time) / 1000 + " secs")
  }
}




