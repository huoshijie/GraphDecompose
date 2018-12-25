/**
  * Created by Administrator on 2018/11/19 0019.
  */
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
object GraphDecomposeObject {

  def GraphDecompose(   CoreNodeCollect:ListBuffer[Long], result:Map[ VertexId,Array[VertexId] ], NodeCollect:Array[VertexId]   ): ListBuffer[Long] ={

    var CandateNode = ListBuffer[VertexId]()
    NodeCollect.toList.copyToBuffer(CandateNode)

    while (CandateNode.size > 0){
      println(CandateNode)
      val Node = CandateNode(0)
      val NodeNeighbor = result(Node).union(ListBuffer[VertexId](Node))
      CoreNodeCollect.append(Node)
      var temp = ListBuffer[VertexId]()
      CandateNode = CandateNode.diff(NodeNeighbor)
      for (elem <- NodeNeighbor){
        if (  (result(elem).intersect(CandateNode).size) > 0  ){
          temp.append(elem)
        }
      }
      CandateNode =  CandateNode.union(temp)
    }
    return CoreNodeCollect
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GraphDecompose").setMaster("local[1]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    var graph = GraphLoader.edgeListFile(sc,"D:\\shujuji\\Toy.txt",numEdgePartitions = 10)

    val degree = graph.degrees

    val neighbor = graph.collectNeighborIds(EdgeDirection.Either)

//    val NodeCollect = degree.sortBy(_._2 , ascending = true).map(_._1).collect()
    val NodeCollect = graph.vertices.map(_._1).collect()

    val result = degree.sortBy(_._2, ascending = false).join(neighbor).map(x=>(x._1,x._2._2)).collect().toMap

    val CoreNodeCollect = ListBuffer[Long]()

    GraphDecompose( CoreNodeCollect, result, NodeCollect)

    println(CoreNodeCollect)
  }
}
