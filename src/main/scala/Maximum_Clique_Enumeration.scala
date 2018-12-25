
import scala.collection.mutable.ListBuffer
import scala.util.control._
class Maximum_Clique_Enumeration {

  def MCE(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)]): Int ={

    val loop = new Breaks;
    var maxClq=0
    for (nodeId <- VertexCollect){

      var nodeIdNeighbor = ListBuffer[Long]()//存储当前节点的邻居节点
      for(edge <- EdgeCollection){
        val x=edge._1
        val y=edge._2
        if(x==nodeId){
          nodeIdNeighbor.append(y)
        }
        if(y==nodeId){
          nodeIdNeighbor.append(x)
        }
      }

//      nodeIdNeighbor.copyToBuffer(nodeIdNeighborCopy)
      if (nodeIdNeighbor.size > maxClq-1) {

        loop.breakable {
          while (nodeIdNeighbor.size >= 0) {

            val max_Clique = ListBuffer[Long]()
            max_Clique.append(nodeId)
            //初始化当前节点为团中的第一个节点
            val nodeIdNeighborNR = ListBuffer[Long]()

            for (negihborNode <- nodeIdNeighbor) {
              //一次循环可以找到一个团

              var temp = true

              loop.breakable {
                for (clique_node <- max_Clique) {
                  //判断negihborNode是否符合最大团定义
                  if ((EdgeCollection.contains((negihborNode, clique_node)) == false) && (EdgeCollection.contains((clique_node, negihborNode)) == false)) {
                    temp = false
                    loop.break()
                  }
                }
              }
              if (temp == true) {
                //代表当前邻居节点可以放入团中
                max_Clique.append(negihborNode)
                nodeIdNeighborNR.append(negihborNode)
              }
            }

            nodeIdNeighbor = nodeIdNeighbor.intersect(nodeIdNeighborNR)


            if (max_Clique.size > maxClq) {
              maxClq = max_Clique.size
            }
            if (nodeIdNeighbor.size < maxClq-1) {
              loop.break()
            }

          }

        }


      }





    }

    maxClq
    }

}
