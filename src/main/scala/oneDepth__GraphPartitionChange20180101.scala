import java.util.Random


import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

object oneDepth__GraphPartitionChange20180101 {
  case class GraphCollect(subGraph_Length:Long)

  def run(parm:Config): Unit ={
    var SubGraphStartTime = System.currentTimeMillis()
//    val conf = new SparkConf().set("spark.driver.maxResultSize","20g").setAppName("GraphPartition").setMaster("spark://10.1.14.20:7077")
    //    val conf = new SparkConf().setAppName("GraphPart4.20ition").setMaster("local[1]")
//    val inPutDir = "/Users/didi/Downloads/GraphPartition/facebook.txt"
//    val outPutDir = "/Users/didi/Downloads/GraphPartition/facebook"
    val conf = new SparkConf().setMaster("spark://10.1.14.20:7077")

    val sparkSession = SparkSession.builder().appName("RDD to oneDepth__GraphPartitionChange20180101")
      .config(conf).getOrCreate()

    val sc = sparkSession.sparkContext

    val g = GraphLoader.edgeListFile(sc,parm.inPutDir,numEdgePartitions = parm.num_partition)

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
    graph.degrees.map(x=>GraphCollect(x._2)).toDF("subGraph_Length").write.mode(SaveMode.Overwrite).parquet(parm.outPutDir)


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
    val set1Graph: Graph[(mutable.HashSet[String]), Int] = g.outerJoinVertices(nbrSets) {
      (vid,attr1,optSet) => (optSet.getOrElse(mutable.HashSet[String]()))
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(triplet: EdgeContext[mutable.HashSet[String], Int, StringBuffer]) {
      val srcneighbor  = triplet.srcAttr
      val dstneighbor  = triplet.dstAttr
      var graph = new StringBuffer("")
      var graph1 = new StringBuffer("")
      graph.append(triplet.srcId)
      graph.append(",")
      graph.append(triplet.dstId)
      graph.append(" ")
      graph.append(triplet.dstId)
      graph.append(",")
      graph.append(triplet.srcId)
      graph.append(" ")

      graph1.append(triplet.srcId)
      graph1.append(",")
      graph1.append(triplet.dstId)
      graph1.append(" ")
      graph1.append(triplet.dstId)
      graph1.append(",")
      graph1.append(triplet.srcId)
      graph1.append(" ")

      val (smallSet, largeSet) = if (srcneighbor.size < dstneighbor.size) {
        (srcneighbor,dstneighbor)
      } else {
        (dstneighbor,srcneighbor)
      }
      val iter = smallSet.iterator
      while (iter.hasNext) {
        val elem = iter.next()
        if (largeSet.contains(elem)) {
          graph.append(elem)
          graph.append(",")
          graph.append(triplet.dstId)
          graph.append(" ")

          graph1.append(elem)
          graph1.append(",")
          graph1.append(triplet.srcId)
          graph1.append(" ")
        }
      }
      triplet.sendToSrc(graph)
      triplet.sendToDst(graph1)
    }
    //graph就是每个点对应的子图
    val subGraph: VertexRDD[StringBuffer] = set1Graph.aggregateMessages(edgeFunc,_.append(_))

    val EversubGraphEdgeCollect = subGraph.map { t =>
      val edgeListBuffer = new ListBuffer[(Long, Long)]
      val nodeListBuffer = new ListBuffer[Long]
      val SubGraphEdgeArray = t._2.toString.trim.split(" ")

      for (elem <- SubGraphEdgeArray) {
        val EveryEdge = elem.split(",")
        edgeListBuffer.append((EveryEdge(0).toLong, EveryEdge(1).toLong))
        nodeListBuffer.append(EveryEdge(0).toLong)
      }
      (nodeListBuffer.distinct.length,nodeListBuffer.distinct,edgeListBuffer.distinct)
    }


//    EversubGraphEdgeCollect.map{x=> GraphCollect(x._1)}.toDF().write.mode(SaveMode.Overwrite).parquet(parm.outPutDir)

    val SubGraphEndTime = System.currentTimeMillis()
    println("求子图时间"+(SubGraphEndTime - SubGraphStartTime).toDouble)



    //以下是求团时间
    //    var StartTime_clique = System.currentTimeMillis()
    val result = EversubGraphEdgeCollect.map{
      x =>
        val cliqueBuffer = new ListBuffer[Long]
        val  VertexCollect= x._2
        val  EdgeCollection = x._3
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
//        Random_BLS_Clque.BLS(VertexCollect,EdgeCollection,clique_best,clique,tabuList,penaltyList,counter_local)
        cliqueBuffer.append(penalty_BLS_Clque.BLS(VertexCollect,EdgeCollection,clique_best,clique,penaltyList,tabuList,counter_local).size)
        cliqueBuffer
    }.flatMap(x=>x).reduce((x,y)=>if (x>y) x else y)
    println(result)

    val EndTime_Clique = System.currentTimeMillis()
    //    println("求团时间"+(EndTime_Clique - StartTime_clique).toDouble)
    println("总时间"+(EndTime_Clique - SubGraphStartTime).toDouble)

        println("单层图划分方法的最大规模是："+ EversubGraphEdgeCollect.map(x=>x._1).reduce((a,b)=>if (a>b) a else b))
        val a = EversubGraphEdgeCollect.map(x=>x._1).reduce((a,b)=>a+b)
        println(a)
        val b = EversubGraphEdgeCollect.count()
        println(b)
        println("单层图划分方法的平均子图规模是："+ (a+0.0)/b)
        val c = EversubGraphEdgeCollect.map{//每一条数据都是都是元组类型，第一个是一个ListBuffer，里边的每一个元素都是Long类型，代表每一个子图所对应的长度，第二个也是ListBuffer，只不过里面每一个元素也都是ListBuffer，表示每个子图的具体的边。两个ListBuffer之间的元素之间是一一对应的
          x=>
            val SubGraph_P = new ListBuffer[Double]
            val Ever_subGraph_Length =  x._1
            val edgeListBuffer = x._2
            val tempA = edgeListBuffer.length/2
            val tempB = Ever_subGraph_Length*(Ever_subGraph_Length-1)/2
            val elem = (tempA+0.0)/tempB
            elem
        }.reduce((e,f)=>e+f)
        println("单层图划分方法的平均子图密度是："+(c+0.0)/b)
        val initEndTime = System.currentTimeMillis()
        sc.stop()
//        println((initEndTime -initStartTime).toDouble)


    println(s"${parm.inPutDir}最大团规模是"+result)

//    EversubGraphEdgeCollect.flatMap(_._1).saveAsTextFile(parm.outPutDir)

    sc.stop()
//    val EndTime_Clique = System.currentTimeMillis()
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
                     outPutDir: String = null,
                     num_partition:Int = 10
                   )

  def getOptParser = {
    val parser = new OptionParser[Config]("generate driver feature order") {
      head("generate driver feature order")
      opt[String]("in_put_dir").required().text("输入路径").
        action((x, c) => c.copy(inPutDir = x))
      opt[String]("out_put_dir").required().text("输出路径").
        action((x, c) => c.copy(outPutDir = x))
      opt[Int]("out_put_dir").required().text("输出路径").
        action((x, c) => c.copy(num_partition = x))
    }
    parser
  }

}
