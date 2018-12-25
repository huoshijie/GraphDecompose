/**
  * Created by Administrator on 2018/11/6 0006.
  */
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext, rdd}
import scopt.OptionParser

import scala.collection.mutable.Map
import scala.util.Random
object LabelPropagationAlgorithm {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LPA").setMaster("local")

    val sc = new SparkContext(conf)

    val path = "D:\\shujuji\\karate.txt"
    var graph = GraphLoader.edgeListFile(sc,path,numEdgePartitions = 10)

    val degree = graph.degrees
    val collectNeighborIds = graph.collectNeighborIds(EdgeDirection.Either)

    val GraphDegreeNeighbor: Graph[(Int,Array[Long]),Int] = graph.outerJoinVertices(degree){
      case(vid,one,degree) => degree.getOrElse(0)
    }.outerJoinVertices(collectNeighborIds){
      case(vid,degree,collectNeighborIds) => (   degree,collectNeighborIds.getOrElse(Array[Long]())  )
    }

    val g = GraphDegreeNeighbor.mapTriplets(triplet=>{
      val srcDegree = triplet.srcAttr._1
      val srcNeighbor = triplet.srcAttr._2

      val dstDegree = triplet.dstAttr._1
      val dstNeighbor = triplet.dstAttr._2

      val commNeighborCount = srcNeighbor.intersect(dstNeighbor).size //公共节点个数

      val Pij = (dstDegree-commNeighborCount-1)/srcDegree
      val Close_ij = 1+commNeighborCount+0.2*Pij

      val Pji = (srcDegree-commNeighborCount-1)/dstDegree
      val Close_ji = 1+commNeighborCount+0.2*Pji

      val attr = (Close_ij,Close_ji)//第一个向源节点发送，第二个向目的节点发送
      attr

    }).mapVertices{
      case (id,(degree,neighbor)) =>
        id //给顶点赋值标签
    }

    val result = g.pregel[  Array[(VertexId,Double)]  ](Array((-1,-1.0)),maxIterations = 100)(

      (id,Label,NewLabel) => {

        if (NewLabel.size == 1 & NewLabel(0)._1 == -1) {
          Label
        }

        else { //标签传播算法
          val map = Map[VertexId, Double]()
          for (elem <- NewLabel) { //得到每一个标签出现的次数

            val label = elem._1
            if (map.contains(label) == false) {
              map(label) = 1
            }
            else {
              map(label) += 1
            }
          }

          val maxLabelNum = map.values.toList.max

          val maxLabelMap = map.filter(x => (maxLabelNum - x._2) / maxLabelNum < 0.4).map(x => (x._1, 0.0))

          for (elem <- NewLabel) {
            val label = elem._1
            val P = elem._2 //节点之间的close值
            if (maxLabelMap.contains(label)) {
              maxLabelMap(label) += P
            }
          }
          val maxLabelP = maxLabelMap.values.toList.max
          Random.shuffle(maxLabelMap.filter(x => x._2 == maxLabelP).keys.toList).take(1)(0) //从关系最紧密的社区中随机选择
        }
      },
      triplet => {
        val srcLabel = triplet.srcAttr
        val dstLabel = triplet.dstAttr
        val srcInfo = triplet.attr._1
        val dstInfo = triplet.attr._2
        val src = Array( (dstLabel,srcInfo) )
        val dst = Array( (srcLabel,dstInfo) )
        Iterator((triplet.srcId, src))
        Iterator((triplet.dstId,dst))

      },

      (a,b) => a++b
    )

    result.vertices.repartition(1).saveAsTextFile("d:\\LPA")

  }

}
