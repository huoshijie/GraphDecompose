/**
  * Created by Administrator on 2018/11/26 0026.
  */

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.collection.mutable.{ListBuffer,Map}
import scala.collection.mutable

object Test_Case {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test_Case").setMaster("local")

    val sc = new SparkContext(conf)

    val neighbor = sc.textFile("D:\\shujuji\\facebook.txt").flatMap{
      x=>
      val edge = x.split("\\s+")
      Array( (edge(0).toLong,edge(1).toLong),(edge(1).toLong,edge(0).toLong) )
    }.distinct().groupByKey().map{
      x=>
        (x._1,x._2.toArray)
    }.collect().toMap

    neighbor.mapValues(_.toBuffer).foreach(println(_))

    val clique = ListBuffer[Long]()
    val all = ListBuffer[ListBuffer[Long]]()
    val cliquebk = new CliqueBK(mutable.Map(neighbor.toSeq: _*),clique,all)
    val SUBG = ListBuffer[VertexId]()
    neighbor.keys.toList.copyToBuffer(SUBG)

    val CAND = ListBuffer[VertexId]()
    neighbor.keys.toList.copyToBuffer(CAND)

    cliquebk.cliqueBK(SUBG,CAND)
    println(all)
    println("极大团个数",all.size)
    println("最大团",all.map(_.size).max)
    println("最大团>54",all.map(_.size).filter(_>54).size)
    println("大于3阶的团个数",all.filter(_.size>3).size)
  }
}
