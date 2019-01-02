/**
  * Created by Administrator on 2018/11/20 0020.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import scala.collection
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GraphDecompose_GraphPartition_CliqueBK {
  def run(): Unit ={
//    parm:Config
    var SubGraphStartTime = System.currentTimeMillis()
//    val conf = new SparkConf().set("spark.driver.maxResultSize","20g").setAppName("GraphPartition").setMaster("spark://10.1.14.20:7077")
    val conf = new SparkConf().setAppName("GraphPartition").setMaster("local[1]")
     //    val inPutDir = "/Users/didi/Downloads/GraphPartition/facebook.txt"
     //    val outPutDir = "/Users/didi/Downloads/GraphPartition/facebook"
     //    val conf = new SparkConf().set("spark.driver.maxResultSize","40g").setAppName("oneDepth__GraphPartitionChange20181120").setMaster("spark://10.1.14.20:7077")
    //    val conf = new SparkConf().set("spark.driver.maxResultSize","40g").setAppName("oneDepth__GraphPartitionChange20181120").setMaster("spark://10.1.14.20:7077")
    val sc = new SparkContext(conf)
    val g = GraphLoader.edgeListFile(sc,"D:\\shujuji\\Toy.txt",numEdgePartitions = 10)
//    val g = GraphLoader.edgeListFile(sc,parm.inPutDir,numEdgePartitions = 10)
    //寻找核心节点
    val degree = g.degrees

    println("最大度数是：",degree.map(_._2).max())
    println("平均度数是：",(degree.map(_._2).reduce(_+_)+0.0)/degree.count())
        
    val neighbor = g.collectNeighborIds(EdgeDirection.Either).map{x=>
      (x._1,x._2.distinct)
    }
    val degreeSortCache = degree.sortBy(_._2, ascending = false).cache()
     val result = degreeSortCache.join(neighbor).map(x=>(x._1,x._2._2)).collect().toMap
//    val NodeCollect = degreeSortCache.map(_._1).collect()
    val NodeCollect = g.vertices.map(_._1).collect()
//    import scala.util.Random
//    var index = 0
//    var sum = 0
//    while(index<10){
//      val CoreNodeCollect = ListBuffer[Long]()
//      GraphDecompose(CoreNodeCollect,result,   Random.shuffle(NodeCollect.toBuffer).take(NodeCollect.size).toArray   )
//      sum += CoreNodeCollect.size
//      index+=1
//    }
//     println("随机打乱顺序的核心节点平均是",(sum+0.0)/index)
    val CoreNodeCollect = ListBuffer[Long]()
    val NodeCollect_degree = degreeSortCache.map(_._1).collect()
    GraphDecompose(CoreNodeCollect,result,NodeCollect_degree)
    println("按照度的顺序排序的核心节点平均是",CoreNodeCollect)
    //    println()
    //将核心节点广播出去
    val bc = sc.broadcast(CoreNodeCollect)
    val graph: Graph[(Array[VertexId],Int), Int] = g.outerJoinVertices(neighbor) {
      (vid,attr1,optSet) => optSet.getOrElse(Array[VertexId]())
    }.mapVertices{
      case (id,attr) =>
        if (bc.value.contains(id)){
          (attr,1)
        }
        else{
          (attr,0)
        }
    }
    //进行诱导子图求解
    def edgeFunc(  triplet: EdgeContext[(Array[VertexId],Int), Int, mutable.Map[VertexId,Array[VertexId]]] ) {
      //      //引入节点的偏序关系
      //      val SrcPreOrder = ListBuffer[VertexId]()
      //      val DstPreOrder = ListBuffer[VertexId]()
      //
      //      val srcId = triplet.srcId
      //      val dstId = triplet.dstId
      //      val srcIndex = CoreNodeCollect.indexOf(srcId)//在核心节点中的位置
      //      val dstIndex = CoreNodeCollect.indexOf(dstId)
      //
      //      for(index <- 0 until srcIndex){ //源点的先序节点集合
      //        SrcPreOrder.append(CoreNodeCollect(index))
      //      }
      //      for (index <- 0 until dstIndex){ //目的顶点的先序节点集合
      //        DstPreOrder.append(CoreNodeCollect(index))
      //      }
      //
      //      if(triplet.srcAttr._2 == 1 & triplet.dstAttr._2 == 1){
      //
      //        if (srcIndex>dstIndex){
      //          val srcneighbor  = triplet.srcAttr._1
      //          val dstneighbor  = triplet.dstAttr._1
      //          var value = triplet.dstId +: srcneighbor.intersect(dstneighbor)
      //          value = value.diff(DstPreOrder)//避免核心节点的先序节点出现
      //
      //          val key = triplet.srcId
      //          triplet.sendToDst(mutable.Map[VertexId,Array[VertexId]](key->value))
      //        }
      //        else {
      //          val srcneighbor  = triplet.srcAttr._1
      //          val dstneighbor  = triplet.dstAttr._1
      //          var value = triplet.srcId +: srcneighbor.intersect(dstneighbor)
      //          value = value.diff(SrcPreOrder)//避免核心节点的先序节点出现
      //
      //          val key = triplet.dstId
      //          triplet.sendToSrc(mutable.Map[VertexId,Array[VertexId]](key->value))
      //        }
      //      }
      //
      //      else
      if (triplet.srcAttr._2 == 1){
        val srcneighbor  = triplet.srcAttr._1
        val dstneighbor  = triplet.dstAttr._1
        var value = triplet.srcId +: srcneighbor.intersect(dstneighbor)
        //        value = value.diff(SrcPreOrder)//避免核心节点的先序节点出现
        val key = triplet.dstId
        triplet.sendToSrc(mutable.Map[VertexId,Array[VertexId]](key->value))
      }
      if (triplet.dstAttr._2 == 1) {
        val srcneighbor  = triplet.srcAttr._1
        val dstneighbor  = triplet.dstAttr._1
        var value = triplet.dstId +: srcneighbor.intersect(dstneighbor)
        //        value = value.diff(DstPreOrder)//避免核心节点的先序节点出现
        val key = triplet.srcId
        triplet.sendToDst(mutable.Map[VertexId,Array[VertexId]](key->value))
      }
    }
    //graph就是每个点对应的子图
    val subGraph: VertexRDD[mutable.Map[VertexId,Array[VertexId]]] = graph.aggregateMessages(edgeFunc,(a,b)=>a++b)

     val finallyResult = graph.outerJoinVertices(subGraph){
      case (vid,(neighbor,flag),opt) =>
        val neighborMap = opt.getOrElse(mutable.Map[VertexId,Array[VertexId]]())
        neighborMap += (vid -> neighbor)
        (neighborMap,flag)
    }
//    println("--------------------------------")
//    for(elem <- finallyResult.vertices.collect()) {
//      println(elem)
//    }
//    println("---------------------------------")
//    println()
//    for (elem <- finallyResult.vertices.collect()(3)._2._1(2)){
//      print(elem," ")
//    }
//    println()
//    for (elem <- finallyResult.vertices.collect()(3)._2._1(5)){
//      print(elem," ")
//    }
//    println()
//    for (elem <- finallyResult.vertices.collect()(3)._2._1(4)){
//      print(elem," ")
//    }
//    println()
//    for (elem <- finallyResult.vertices.collect()(3)._2._1(3)){
//      print(elem," ")
//    }
//    println()
//    for (elem <- finallyResult.vertices.collect()(3)._2._1(6)){
//      println(elem)
//    }
    val CliqueResult = finallyResult.vertices.filter(_._2._2==1).repartition(25).flatMap{
  x=>
    val clique = ListBuffer[Long]()
    val all = ListBuffer[ListBuffer[Long]]()
    val neighbor = x._2._1
    val SUBG = ListBuffer[VertexId]()
    x._2._1.keys.toList.copyToBuffer(SUBG)
    val CAND = ListBuffer[VertexId]()
    x._2._1.keys.toList.copyToBuffer(CAND)
//    val Index = bc.value.indexOf(x._1)
    val  Del = ListBuffer[Long]()
//    for(index <- 0 until Index){ // 得到当前节点的删除节点集合
//      Del.append(bc.value(index))
//    }
    var index = 0
    val elem = bc.value(index)
    while (elem != x._1){
        Del.append(elem)
        index+=1
    }
    val cliqueBK = new CliqueBK(neighbor,clique,all)
//    cliqueBK.cliqueBK(SUBG,CAND.diff(Del))
    cliqueBK.cliqueBK(SUBG,CAND)
    all
}.cache()
    val cliqueCount =  CliqueResult.count()
    println(cliqueCount)
    val all = CliqueResult.map(x=>x.sorted).collect().distinct
    println("---",all.toBuffer)
    println("极大团个数",all.size)
    println("最大团",all.map(_.size).max)
    println("大于3阶的团个数",all.filter(_.size>=3).size)
//    CliqueResult.saveAsTextFile(parm.outPutDir)
    sc.stop()
  }

//  def GraphDecompose(   CoreNodeCollect:ListBuffer[Long], result:Map[ VertexId,Array[VertexId] ], NodeCollect:Array[VertexId]   ): ListBuffer[Long] ={
//    var CandateNode = ListBuffer[VertexId]()
//    NodeCollect.toList.copyToBuffer(CandateNode)
//    while (CandateNode.size > 0){
//      val Node = CandateNode(0)
//      val NodeNeighbor = result(Node).union(ListBuffer[VertexId](Node))
//      CoreNodeCollect.append(Node)
//      var temp = ListBuffer[VertexId]()
//
//      CandateNode = CandateNode.diff(NodeNeighbor)
//
//      for (elem <- NodeNeighbor){
//        if (  (result(elem).intersect(CandateNode).size) > 0  ){
//          temp.append(elem)
//        }
//      }
//      CandateNode =  CandateNode.union(temp)
//    }
//    return CoreNodeCollect
//  }
def GraphDecompose( CoreNodeCollect:ListBuffer[Long], result:Map[ VertexId,Array[VertexId] ], NodeCollect:Array[VertexId]): ListBuffer[Long] ={ //图分解的第二个版本
  var index = 0
  val LabelFlag = mutable.Map[VertexId,Int]()
  for (node <- NodeCollect){
    val flag = LabelFlag.getOrElse(node,-1)
    val neighbor = result(node) //得到当前节点的全部邻居节点
    if (flag == -1){ //当前节点没有label
      val label = (index*2+1)
      LabelFlag(node) = label //当前节点标记为强标签
      CoreNodeCollect.append(node) //当前节点是核心节点
      for (elem <- neighbor){     //开始对当前节点的邻居节点标上弱标签
        if(LabelFlag.getOrElse(elem,-1) == -1){
          LabelFlag(elem) = label*10
        }
      }
    }
    if(flag%10==0){ //说明当前节点是弱标签
      for (elem <- neighbor){
        if (LabelFlag.getOrElse(elem,-1) != -1 ){
          if (LabelFlag(elem)%10 ==0 & LabelFlag(elem) != flag){        //与当前节点标签相同的邻居节点 之间的边去掉
            LabelFlag(elem) = flag/10                                   //给标签不一样的邻居节点赋值为强标签
            CoreNodeCollect.append(elem)
          }
        }
      }
    }
    index+=1
    //当前节点是强标签是不做任何操作
  }
  CoreNodeCollect
}

  def main(args: Array[String]): Unit = {
//    val defaultParams = Config()
//    val parser = getOptParser
//    parser.parse(args, defaultParams) match {
//      case Some(param) => run(param)
//    }
run()
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



