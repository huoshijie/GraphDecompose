/**
  * Created by Administrator on 2018/11/6 0006.
  */
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext, rdd}
import scopt.OptionParser
import scala.collection.mutable.Map
import scala.util.Random
import scala.Array._
class LabelPropagationAlgorithm {

  def ComputeModularity(result:Graph[VertexId,(Double,Double)],sc:SparkContext): Double ={

    val NeighborRDD  = result.collectNeighborIds(EdgeDirection.Either).map(x=>(x._1,x._2.distinct)).cache()
    val m = NeighborRDD.map(_._2.size).reduce(_+_)/2 //图中边的个数
//    println("边的个数为"+m)
    val degree = NeighborRDD.map{
      x=>
        val degree = x._2.size
        (x._1,degree)
    }
    val Dc = result.vertices.join(degree).map{
      x=>
        x._2
    }.reduceByKey(_+_)

    //RDD,(Label,drgreeSumBaseLabel)

//    println()
//    println("Dc的值为")
//    Dc.foreach(println(_))

    val Node2Neighbor = NeighborRDD.collectAsMap()
    val bc = sc.broadcast(Node2Neighbor)
    val Lc = result.vertices.map{x=>
       (x._2,x._1)
     }.groupByKey().map{
       x=>
         val nodeColl = x._2.toArray
         val label = x._1
         var Lc = 0
         for(a <- nodeColl){
           for (b<- nodeColl){
             if (  bc.value(a).contains(b)){//社区内部的顶点之间有边相连
               Lc+=1
             }
           }
         }
         (label,Lc/2)
     }
//    println()
//    println("Lc的值为")
//    Lc.foreach(println(_))

    val Modularity = Lc.join(Dc).map{
       x=>
         val Lc = x._2._1
         val Dc = x._2._2
         val value = (Lc+0.0)/m-((Dc+0.0)/(2*m))*((Dc+0.0)/(2*m))
         value
     }.reduce(_+_)
    Modularity
  }

  def PS_LabelPropagationAlgorithm(graph:Graph[VertexId,Int],sc:SparkContext,Iterations:Int): Graph[VertexId,(Double,Double)] ={
    val collectNeighborIds = graph.collectNeighborIds(EdgeDirection.Either)
    val GraphDegreeNeighbor: Graph[(VertexId,Array[Long]),Int] = graph.outerJoinVertices(collectNeighborIds){
      case (vid,label,neighbor) =>  (label,neighbor.getOrElse(Array[Long]()).distinct)
    }
    val g = GraphDegreeNeighbor.mapTriplets(triplet=>{
      val srcDegree = triplet.srcAttr._2.size
      val srcNeighbor = triplet.srcAttr._2
      val dstDegree = triplet.dstAttr._2.size
      val dstNeighbor = triplet.dstAttr._2
      val commNeighborCount = srcNeighbor.intersect(dstNeighbor).size //公共节点个数
      val Pij = (dstDegree-commNeighborCount-1)/srcDegree
      val Close_ij = 1+commNeighborCount+0.2*Pij
      val Pji = (srcDegree-commNeighborCount-1)/dstDegree
      val Close_ji = 1+commNeighborCount+0.2*Pji
      val attr = (Close_ij,Close_ji)//第一个向源节点发送，第二个向目的节点发送
      attr
    }).mapVertices{
      case (vid,attr) =>
        attr._1
    }

    val result = g.pregel[  Array[(VertexId,VertexId,Double)]  ](Array((-1,-1,-1.0)),maxIterations = Iterations)(
      (id,Label,NewLabel) => {
        if (NewLabel.size == 1 & NewLabel(0)._1 == -1) {
          Label
        }
        else { //标签传播算法
          val map = Map[VertexId, Double]()
          for (elem <- NewLabel.distinct) { //得到每一个标签出现的次数
            val label = elem._2
            if (map.contains(label) == false) {
              map(label) = 1
            }
            else {
              map(label) += 1
            }
          }
          val maxLabelNum = map.values.toList.max
          val maxLabelMap = map.filter(x => (maxLabelNum - x._2) / maxLabelNum < 0.4).map(x => (x._1, 0.0))
          for (elem <- NewLabel.distinct) {
            val label = elem._2
            val P = elem._3 //节点之间的close值
            if (maxLabelMap.contains(label)) {
              maxLabelMap(label) += P
            }
          }
          val maxLabelP = maxLabelMap.values.toList.max
          Random.shuffle(maxLabelMap.filter(x => x._2 == maxLabelP).keys.toList).take(1)(0) //从关系最紧密的社区中随机选择
        }
      },
      triplet =>{
        val srcLabel = triplet.srcAttr
        val dstLabel = triplet.dstAttr
        val srcInfo = triplet.attr._1
        val dstInfo = triplet.attr._2
        val src = Array( (triplet.dstId,dstLabel,srcInfo) )
        val dst = Array( (triplet.srcId,srcLabel,dstInfo) )

        Iterator( (triplet.dstId, dst)  )
        Iterator( (triplet.srcId, src)  )
      },
      (a,b) => a++b
    )
    result
//    result.vertices.foreach(println(_))
//    ComputeModularity(result,sc)
  }

  def LabelPropagationAlgorithm(graph:Graph[VertexId,Int],sc:SparkContext,Iterations:Int): Graph[VertexId,(Double,Double)] ={

    val result = graph.pregel[  Array[(VertexId)]  ](Array(-1),maxIterations = Iterations)(
      (id,Label,NewLabel) => {
        if (NewLabel.size == 1 & NewLabel(0) == -1) {
          Label
        }
        else { //标签传播算法
          val map = Map[VertexId, Double]()
          for (elem <- NewLabel.distinct) { //得到每一个标签出现的次数
            val label = elem
            if (map.contains(label) == false) {
              map(label) = 1
            }
            else {
              map(label) += 1
            }
          }
          val maxLabelNum = map.values.toList.max

          Random.shuffle(map.filter(x => x._2 == maxLabelNum).keys.toList).take(1)(0) //从关系最紧密的社区中随机选择
        }
      },
      triplet =>{
        val dstLabel = triplet.dstAttr
        val src = Array(dstLabel)
        Iterator( (triplet.srcId, src)  )
      },
      (a,b) => a++b
    )
    result.mapTriplets(triplets=>(-1.0,-1.0))
    //    result.vertices.foreach(println(_))
//    ComputeModularity(result.mapTriplets(triplets=>(-1.0,-1.0)),sc)
  }

  def ComputeNMI(result:Graph[VertexId,(Double,Double)],sc:SparkContext,realPartitionPath:String): Double ={
    val n = result.numVertices

    val algorithm_partition = result.vertices.map{
      case (vid,label) =>
        (label,vid)
    }.groupByKey().mapValues(_.toList).collect()
    val CB = algorithm_partition.size //算法划分类别
    val real_partiton = sc.textFile(realPartitionPath).map{
      x=>
        val arr = x.split("\\s+")
        val vid = arr(0).toLong
        val label = arr(1).toLong
        (label,vid)
    }.groupByKey().mapValues(_.toList).collect()
    val CA = real_partiton.size
    var N = ofDim[Int](CA,CB)


    for(i <- 0 until(CA)){
      for (j <- 0 until(CB)){
        N(i)(j) = (real_partiton(i)._2).intersect(algorithm_partition(j)._2).size
      }
    }
    var partA = 0.0
    var partB = 0.0
    var partC = 0.0

    for(i <- 0 until(CA)) {
      val i_row_sum = N(i).sum
      partB += (i_row_sum)*math.log(  (i_row_sum+0.0)/n   )

    }
    for (j <- 0 until (CB)) {

      val j_col_sum = {
        var temp = 0
        for(k <- 0 until(CA)){
          temp = temp + N(k)(j)
        }
        temp
      }//第j列求和
      partC+= (j_col_sum)*math.log(   (j_col_sum+0.0)/n  )
    }

    for(i <- 0 until(CA)) {
      for (j <- 0 until (CB)) {

        val i_row_sum = N(i).sum
        val j_col_sum = {
          var temp = 0
          for(k <- 0 until(CA)){
            temp = temp + N(k)(j)
          }
          temp
        }//第j列求和

        partA+= (if (N(i)(j)==0) 0 else N(i)(j)*math.log(   (N(i)(j)*n+0.0)/(i_row_sum*j_col_sum)  ))
      }
    }

    val NMI = -2*partA/(partB+partC)
    NMI
  }

  //  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("LPA").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val path = "D:\\shujuji\\karate.txt"
//    var graph = GraphLoader.edgeListFile(sc,path,numEdgePartitions = 10)
//
////    val collectNeighborIds = graph.collectNeighborIds(EdgeDirection.Either)
////
////    val GraphDegreeNeighbor: Graph[Array[Long],Int] = graph.outerJoinVertices(collectNeighborIds){
////      case (vid,defult,neighbor) =>  neighbor.getOrElse(Array[Long]()).distinct
////    }
////    val g = GraphDegreeNeighbor.mapTriplets(triplet=>{
////      val srcDegree = triplet.srcAttr.size
////      val srcNeighbor = triplet.srcAttr
////      val dstDegree = triplet.dstAttr.size
////      val dstNeighbor = triplet.dstAttr
////      val commNeighborCount = srcNeighbor.intersect(dstNeighbor).size //公共节点个数
////      val Pij = (dstDegree-commNeighborCount-1)/srcDegree
////      val Close_ij = 1+commNeighborCount+0.2*Pij
////      val Pji = (srcDegree-commNeighborCount-1)/dstDegree
////      val Close_ji = 1+commNeighborCount+0.2*Pji
////      val attr = (Close_ij,Close_ji)//第一个向源节点发送，第二个向目的节点发送
////      attr
////    }).mapVertices{
////      case (id,neighbor) =>
////        id //给顶点赋值标签
////    }
////
////    for (index <- 1 to 100){
////      LabelPropagationAlgorithm(g,sc,index)
//    }
//  }
}
