import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.Random

class CliqueBK(neighbor:mutable.Map[Long,Array[Long]],clique:ListBuffer[Long], all:ListBuffer[ListBuffer[Long]] ) {

  def cliqueBK(   SUBG:ListBuffer[Long], CAND:ListBuffer[Long]   ):Unit ={
    if (SUBG.size==0){
      val x = ListBuffer[Long]()
      clique.copyToBuffer(x)
      all.append(x)
      return clique
    }
    else {
      var max = -1
      var u = Long.MaxValue
      for (v <- SUBG){ //从SUBG中选出一个点，使得 CAND与u的邻居节点的交集最大
        val temp = CAND.intersect(neighbor(v)).size
        if (temp>max){
          max = temp
          u = v
        }
      }
      var expend = CAND.diff( neighbor(u) )
      while (expend.size>0){
        val q = expend(new Random().nextInt(expend.size) )
        clique.append(q)
        val SUBGq = SUBG.intersect(neighbor(q))
        val CANDq = CAND.intersect(neighbor(q))
        cliqueBK(SUBGq,CANDq)
        if (CAND.contains(q))   CAND.remove(CAND.indexOf(q))
        clique.remove(clique.indexOf(q))
        expend = CAND.diff( neighbor(u) )
      }
    }
    return None
  }
}

