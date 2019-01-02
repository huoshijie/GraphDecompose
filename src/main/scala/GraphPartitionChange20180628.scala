import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql._
import scopt.OptionParser

import scala.collection.mutable.{ListBuffer, Map}
import scala.collection.{immutable, mutable}

object GraphPartitionChange20180628 {

  case class GraphCollect(Ever_subGraph_Length:ListBuffer[Long],Ever_subGraph_Vertex:ListBuffer[ListBuffer[Long]],vertex2subGraph:ListBuffer[ListBuffer[(Long,Long)]],SubGraph_P:ListBuffer[Double])
  case class subGraph_Length(Ever_subGraph_Length:ListBuffer[Long])

  def run(parm:Config): Unit ={

    var SubGraphStartTime = System.currentTimeMillis()
//    val conf = new SparkConf().set("spark.driver.maxResultSize","20g").setAppName("GraphPartition").setMaster("spark://10.1.14.20:7077")
    //    val conf = new SparkConf().setAppName("GraphPart4.20ition").setMaster("local[1]")
//    val inPutDir = "/Users/didi/Downloads/GraphPartition/facebook.txt"
//    val outPutDir = "/Users/didi/Downloads/GraphPartition/facebook"
    val conf = new SparkConf().setMaster("spark://10.1.14.20:7077")
    val sparkSession = SparkSession.builder().appName("RDD to DataFrame").config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    val g = GraphLoader.edgeListFile(sc,parm.inPutDir,numEdgePartitions = 10)
    //    val g = GraphLoader.edgeListFile(sc,"D:\\shujuji\\graph\\facebook_combined.txt",numEdgePartitions = 1)
    //做文本预处理：
    //    sc.textFile(args(0)).filter(line =>line.isEmpty && line(0) != '#').map{line =>
    //          val lineArray = line.split("\\s+")
    //          if (lineArray.length < 2) {
    //            throw new IllegalArgumentException("Invalid line: " + line)
    //          }
    //          val srcId = lineArray(0).toLong
    //          val dstId = lineArray(1).toLong
    //          (srcId,dstId)
    //      }.filter(x=>x._1==x._2).map(x=>x._1+" "+x._2).saveAsTextFile(args(1))
    //    val g = GraphLoader.edgeListFile(sc,args(1),numEdgePartitions = 1)
    // Construct set representations of the neighborhoods

    val nbrSets: VertexRDD[mutable.HashSet[String]] =
    g.collectNeighborIds(EdgeDirection.Either).mapValues{(vid, nbrs) =>
      val set = new mutable.HashSet[String]()
      var i = 0
      while (i < nbrs.size) {
        if (nbrs(i) != vid) {
          set.add(nbrs(i).toString)
        }
        i += 1
      }
      set
    }

    val graph:Graph[Int,Int] = g.outerJoinVertices(g.degrees) {
      case(vid,one,degree) => degree.getOrElse(0)
    }
    //    println("单层图划分方法的最大规模是："+(g.degrees.reduce((a,b)=> if(a._2>b._2) a else b))._2)
    //    println("单层图划分方法的平均子图规模是："+(g.degrees.map(_._2).reduce((a,b)=>a+b)))
    g.unpersist()
    val olderFollowers = graph.aggregateMessages[String](
      triplet =>{
        if ((triplet.srcAttr<triplet.dstAttr) ||((triplet.srcAttr==triplet.dstAttr)&&(triplet.srcId<triplet.dstId)))
        {triplet.sendToSrc(triplet.dstId+" ")}
        if ((triplet.srcAttr>triplet.dstAttr) ||((triplet.srcAttr==triplet.dstAttr)&&(triplet.srcId>triplet.dstId)))
        {triplet.sendToDst(triplet.srcId+" ")}
      },
      (a,b) =>
      {a+b}
    ).map{x=>
      val field = x._2.trim().split(" ")
      val newfield  = field.distinct.toSet
      (x._1,newfield)
    }
    //    olderFollowers.map(x => (x._2).size).saveAsTextFile(args(1))
    val set1Graph: Graph[(Int,mutable.HashSet[String]), Int] = graph.outerJoinVertices(nbrSets) {
      (vid, degreeRdd, optSet) => (degreeRdd,optSet.getOrElse(mutable.HashSet[String]()))
    }
    val set2Graph:Graph[(Int,mutable.HashSet[String],immutable.Set[String]),Int] = set1Graph.outerJoinVertices(olderFollowers){
      case(vid, (degreeRdd,neighbor),eligible_neighbor)=>(degreeRdd,neighbor,eligible_neighbor.getOrElse(immutable.Set[String]()))
    }
    graph.unpersist()
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(triplet: EdgeContext[(Int,mutable.HashSet[String],immutable.Set[String]), Int, StringBuffer]) {
      val srcneighbor_degree = triplet.srcAttr._1
      val dstneighbor_degree = triplet.dstAttr._1
      val srcneighbor  = triplet.srcAttr._2
      val dstneighbor  = triplet.dstAttr._2
      if ((srcneighbor_degree < dstneighbor_degree) || ((srcneighbor_degree == dstneighbor_degree) && (triplet.srcId < triplet.dstId))) {
        var graph = new StringBuffer("")
        graph.append(triplet.srcId)
        graph.append(",")
        graph.append(triplet.dstId)
        graph.append(" ")
        graph.append(triplet.dstId)
        graph.append(",")
        graph.append(triplet.srcId)
        graph.append(" ")
        val eligible_neighbor = triplet.srcAttr._3
        val (smallSet, largeSet) = if (eligible_neighbor.size < dstneighbor.size) {
          (eligible_neighbor,dstneighbor)
        } else {
          (dstneighbor,eligible_neighbor)
        }
        val iter = smallSet.iterator
        while (iter.hasNext) {
          val elem = iter.next()
          if (largeSet.contains(elem)) {
            graph.append(elem)
            graph.append(",")
            graph.append(triplet.dstId)
            graph.append(" ")
          }
        }
        triplet.sendToSrc(graph)
      }
      if ((srcneighbor_degree > dstneighbor_degree) || ((srcneighbor_degree == dstneighbor_degree) && (triplet.srcId > triplet.dstId))) {
        var graph =  new StringBuffer("")
        graph.append(triplet.srcId)
        graph.append(",")
        graph.append(triplet.dstId)
        graph.append(" ")
        graph.append(triplet.dstId)
        graph.append(",")
        graph.append(triplet.srcId)
        graph.append(" ")
        val eligible_neighbor = triplet.dstAttr._3
        val (smallSet, largeSet) = if (eligible_neighbor.size < srcneighbor.size) {
          (eligible_neighbor,srcneighbor)
        } else {
          (srcneighbor,eligible_neighbor)
        }
        val iter = smallSet.iterator
        while (iter.hasNext) {
          val elem = iter.next()
          if (largeSet.contains(elem)) {
            graph.append(elem)
            graph.append(",")
            graph.append(triplet.srcId)
            graph.append(" ")
          }
        }
        triplet.sendToDst(graph)
      }
    }
    //graph就是每个点对应的子图
    val subGraph: VertexRDD[StringBuffer] = set2Graph.aggregateMessages(edgeFunc,_.append(_))
    set1Graph.unpersist()
    set2Graph.unpersist()
    val EversubGraphEdgeCollect = subGraph.map {t =>
      val edgeListBuffer = new ListBuffer[(Long, Long)]
      val vertex2subGraph = new ListBuffer[ListBuffer[(Long,Long)]]//这是最终的返回值，生成每一个点和每一个点的子图

    val nodeListBuffer = new ListBuffer[Long]//存储全部节点的ID
    val vertex2Neighbor = mutable.Map[Long,ListBuffer[Long]]()//顶点 到 全部的邻接节点
    val vertex2Degree = mutable.Map[Long,Int]()//顶点 到 顶点的度数
    val SubGraphEdgeArray = t._2.toString.trim.split(" ")

      val Ever_subGraph_Length = new ListBuffer[Long]
      val Ever_subGraph_Vertex = new ListBuffer[ListBuffer[Long]]
      for (elem <- SubGraphEdgeArray) {
        val EveryEdge = elem.split(",")
        edgeListBuffer.append((EveryEdge(0).toLong, EveryEdge(1).toLong))
        nodeListBuffer.append(EveryEdge(0).toLong)
      }//(1,ListBuffer((1,2), (2,1), (3,2), (2,3), (4,2), (2,4), (1,3), (3,1), (4,3), (3,4), (1,4), (4,1)))
      if (nodeListBuffer.distinct.length > 300){
        for(elem <- nodeListBuffer.distinct){
          val vertexIdListBuffer = new ListBuffer[Long]
          for(edgeElem <- edgeListBuffer.distinct)
          {
            if (edgeElem._1 == elem){
              vertexIdListBuffer.append(edgeElem._2)
            }
          }
          vertex2Neighbor += (elem -> vertexIdListBuffer)
          vertex2Degree += (elem -> vertexIdListBuffer.length)
        }
        for (vi <- nodeListBuffer.distinct){
          val V_i_List = new ListBuffer[Long]//存储vi节点满足条件的邻接节点
          val subgraph_Listbuffer  = new ListBuffer[(Long,Long)]()
          for (neighbor <- vertex2Neighbor(vi)) {//这个循环是寻找vi节点周围的邻居节点中度数比自己大或者度数相等但节点ID比自己大的邻居节点
            if ((vertex2Degree(neighbor)>vertex2Degree(vi)) || (vertex2Degree(neighbor)==vertex2Degree(vi) && neighbor>vi)){
              V_i_List.append(neighbor)
            }
          }
          for(vertex <- V_i_List){//这个循环针对vi是生成子图
            subgraph_Listbuffer.append((vi,vertex))
            subgraph_Listbuffer.append((vertex,vi))
            val vertexList = vertex2Neighbor(vertex)//选择viList中的节点vertex，得到vertex的邻接点集合vertexList。
            for (tempV <- vertexList){
              if((tempV != vi) && V_i_List.contains(tempV)){//满足((tempV∈viList)∩(tempV≠vi))，将tempV与vertex相连，
                subgraph_Listbuffer.append((tempV,vertex))
              }
            }
          }
          if(V_i_List.length!=0)
          {
            V_i_List.append(vi)
            Ever_subGraph_Vertex.append(V_i_List)
            Ever_subGraph_Length.append(V_i_List.length+1)//加1是为了包括当前节点
            vertex2subGraph.append(subgraph_Listbuffer)
          }
        }

        val SubGraph_P = new ListBuffer[Double]//存储各个子图的节点密度

        for (index <- 0 until Ever_subGraph_Length.length) {
          val tempA = vertex2subGraph(index).distinct.size
          val tempB = Ever_subGraph_Length(index)*(Ever_subGraph_Length(index)-1)/2
          val elem = (tempA+0.0)/tempB
          SubGraph_P.append(elem)
        }
        (Ever_subGraph_Length,Ever_subGraph_Vertex,vertex2subGraph,SubGraph_P)

      }
      else {

        Ever_subGraph_Length.append(nodeListBuffer.distinct.length)//各个子图的顶点个数
        Ever_subGraph_Vertex.append(nodeListBuffer.distinct)
        vertex2subGraph.append(edgeListBuffer.distinct)

        val SubGraph_P = new ListBuffer[Double]//存储各个子图的节点密度

        for (index <- 0 until Ever_subGraph_Length.length) {
          val tempA = vertex2subGraph(index).distinct.size/2
          val tempB = Ever_subGraph_Length(index)*(Ever_subGraph_Length(index)-1)/2
          val elem = (tempA+0.0)/tempB
          SubGraph_P.append(elem)
        }
       (Ever_subGraph_Length,Ever_subGraph_Vertex,vertex2subGraph,SubGraph_P)

      }
    }.cache()

//    val array100 = EversubGraphEdgeCollect.filter(x=> x._1(0)==229).take(1)
//
//    val cliqueBuffer = new ListBuffer[Long]
//
//    val VertexCollect = array100(0)._2(0)
//    val EdgeCollection=  array100(0)._3(0)
//    var clique_best =  ListBuffer[Long]()
//    val clique = ListBuffer[Long]()
//    val tabuList = Map[Long,Int]()
//    val penaltyList = Map[Long,Int]()
//    val counter_local = 0
//    val V1 =VertexCollect((new Random).nextInt(VertexCollect.size)) //随机选择一个点形成当前团
//
//   clique.append(V1)
//
//
//    clique.copyToBuffer(clique_best)//将当前团赋给当前的全局最优
//    for(vertex <- VertexCollect){
//      tabuList += (vertex->0)
//      penaltyList+=(vertex->0)
//    }
//    penaltyList(V1)+=1
//    val Random_BLS_Clque = new Random_BLS()
//    val penalty_BLS_Clque = new Penalty_BLS()
//
//    //          Random_BLS_Clque.BLS(VertexCollect,EdgeCollection,clique_best,clique,tabuList,penaltyList,counter_local)
//    cliqueBuffer.append(penalty_BLS_Clque.BLS(VertexCollect,EdgeCollection,clique_best,clique,penaltyList,tabuList,counter_local).size)
//    val SubGraphEndTime = System.currentTimeMillis()
//    println("求子图时间"+(SubGraphEndTime - SubGraphStartTime).toDouble)
//
//
//
//    val max_SubGraph = EversubGraphEdgeCollect.flatMap(_._1).reduce((a,b)=> if(a>b) a else b)
//    EversubGraphEdgeCollect.flatMap(_._1)
//
//
//    println("Multiple图划分方法的最大规模是:"+ max_SubGraph)
//    val a = EversubGraphEdgeCollect.flatMap(_._1).reduce((a,b)=> a+b)
//    val b = EversubGraphEdgeCollect.flatMap(_._1).count
//
//    println("Multiple图划分方法的平均子图规模是"+(a+0.0)/b)
//    val c = EversubGraphEdgeCollect.flatMap{//每一条数据都是都是元组类型，第一个是一个ListBuffer，里边的每一个元素都是Long类型，代表每一个子图所对应的长度，第二个也是ListBuffer，只不过里面每一个元素也都是ListBuffer，表示每个子图的具体的边。两个ListBuffer之间的元素之间是一一对应的
//      x=>
//        val SubGraph_P = new ListBuffer[Double]
//        val Ever_subGraph_Length =  x._1
//        val edgeListBuffer = x._3
//        for (index <- 0 until Ever_subGraph_Length.length) {
//          val tempA = edgeListBuffer(index).distinct.size/2
//          val tempB = Ever_subGraph_Length(index)*(Ever_subGraph_Length(index)-1)/2
//          val elem = (tempA+0.0)/tempB
//          SubGraph_P.append(elem)
//        }
//        SubGraph_P
//    }.reduce((a,b)=>a+b)
//
//    println("PGP方法的平均子图密度为"+c/(b+0.0))
    //以下是求团时间
    var StartTime_clique = System.currentTimeMillis()
    var result = EversubGraphEdgeCollect.map{
      x =>
        val cliqueBuffer = new ListBuffer[ListBuffer[Long]]
        val SubGraph_Vertex_Collection = x._2
        val SubGraph_Edge_Collection = x._3
        for(i <- 0 until SubGraph_Edge_Collection.size){
          val EdgeCollection = SubGraph_Edge_Collection(i)
          val VertexCollect =  SubGraph_Vertex_Collection(i)
//          if(VertexCollect.size>200){
//            val mce = new Maximum_Clique_Enumeration()
//            val localmaxCliqueSize = mce.MCE(VertexCollect,EdgeCollection)
//            cliqueBuffer.append(localmaxCliqueSize)
//          }
//          if(VertexCollect.size>2){
            var clique_best = new ListBuffer[Long]
            val clique = new ListBuffer[Long]
            val tabuList = Map[Long,Int]()
            val penaltyList = Map[Long,Int]()
            val counter_local = 0
            val V1 =VertexCollect((new Random).nextInt(VertexCollect.size)) //随机选择一个点形成当前团
            clique.append(V1)
            clique.copyToBuffer(clique_best)//将当前团赋给当前的全局最优
            for(vertex <- VertexCollect){
              tabuList += (vertex->0)
              penaltyList+=(vertex->0)
            }
            penaltyList(V1)+=1
            val Random_BLS_Clque = new Random_BLS()
            val penalty_BLS_Clque = new Penalty_BLS()
            cliqueBuffer.append(penalty_BLS_Clque.BLS(VertexCollect,EdgeCollection,clique_best,clique,penaltyList,tabuList,counter_local))
          }
//        }
        cliqueBuffer
    }.flatMap(x=>x).map(_.sorted).map{
        list =>
          for(elem <- list) yield (elem, list)

      }.flatMap(x=>x).groupByKey().map{
        x=>
          val listCollect = x._2.toList
          val clique = listCollect.reduce((a,b)=>if (a.size>b.size) a else b)
           clique
      }.cache()
    result.filter(_.size>2).repartition(1).saveAsTextFile(parm.outPutDir)
    val resultOne = result.filter(_.size>2).collect()

    println("一共找到了"+resultOne.size+"个团，应该和节点个数相同")
    println("去重后一共有"+resultOne.distinct.size+"个团")
    println("杰哥，你已经测试成功了")
//      .reduce((x,y)=>if (x>y) x else y)

//    println(s"${parm.inPutDir}最大团规模是"+result)


//    EversubGraphEdgeCollect.flatMap(_._1).saveAsTextFile(parm.outPutDir)

    sc.stop()
    val EndTime_Clique = System.currentTimeMillis()
    //    println("求团时间"+(EndTime_Clique - StartTime_clique).toDouble)
    println("总时间"+(EndTime_Clique - SubGraphStartTime).toDouble)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Config()
    val parser = getOptParser
    parser.parse(args, defaultParams) match {
      case Some(param) => run(param)
    }
  }

  case class Config(
                     inPutDir: String = null,
                     outPutDir: String = null
                   )


  def getOptParser = {
    val parser = new OptionParser[Config]("generate driver feature order") {
      head("generate driver feature order")

      opt[String]("in_put_dir").required().text("输入路径").
        action((x, c) => c.copy(inPutDir = x))

      opt[String]("out_put_dir").required().text("输出路径").
        action((x, c) => c.copy(outPutDir = x))

    }
    parser
  }
}