import scala.collection.mutable.{ListBuffer, Map}
import scala.util.Random
import scala.util.control.Breaks

class Penalty_BLS {

  def pertub_dir(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique:ListBuffer[Long],penaltyList:Map[Long,Int],tabuList:Map[Long,Int], clique_best:ListBuffer[Long]): Unit ={
    val loop = new Breaks;
    val tabu_PA_out = new ListBuffer[Long]//pa中未被禁忌的点
    val tabu_OM_out = new ListBuffer[Long]//om中未被禁忌的点
    val penaltyMax_out = new ListBuffer[Long]
    val PA = get_ecandidate(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique:ListBuffer[Long])
    val OM = get_Pcandidate(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique:ListBuffer[Long])
    var sign_dir = 0
    var minpenalty =  1000
    for (elem<- PA)
    {
      if (penaltyList(elem)<minpenalty){
        minpenalty = penaltyList(elem)
      }
    }
    if(PA.isEmpty==false){//进行加点操作

      for (elem <- PA){
        if(tabuList(elem)==0 && penaltyList(elem)==minpenalty){
          tabu_PA_out.append(elem)
        }
      }
      if(tabu_PA_out.isEmpty==false){
        val v = tabu_PA_out((new Random).nextInt(tabu_PA_out.size))
        clique.append(v)
        penaltyList(v)+=1
        sign_dir=1
      }
      else if(clique.size>=clique_best.size) {
        val v = PA((new Random).nextInt(PA.size))
        clique.append(v)
        penaltyList(v)+=1
        sign_dir=1
      }
    }
    if((sign_dir!=1)&&(OM.isEmpty==false)){//进行换点操作
    var v:Long = -1
      for(elem <- OM){
        if (tabuList(elem) == 0 && penaltyList(elem)==minpenalty){
          tabu_OM_out.append(elem)
        }
      }
      if (tabu_OM_out.isEmpty==false){
        v = tabu_OM_out((new Random).nextInt(tabu_OM_out.size))
        sign_dir = 1
      }
      else if(clique.size>=clique_best.size){
        v = OM((new Random).nextInt(OM.size))
        sign_dir = 1
      }
      if(sign_dir==1){
        loop.breakable {
          for (vertex <- clique){
            val temp = (v,vertex)
            if(EdgeCollection.contains(temp)==false) {
              tabuList(vertex) = 7+(new Random).nextInt(OM.size)
              clique.remove(clique.indexOf(vertex))
              clique.append(v)
              penaltyList(v)+=1
              loop.break
            }
          }
        }
      }
    }
    if(sign_dir!=1 && clique.isEmpty==false){//进行删点操作

      var Max_penalty = 0
      for(elem <- clique){
        if (penaltyList(elem)>Max_penalty){
          Max_penalty = penaltyList(elem)
        }
      }
      for (elem<-clique){
        if(penaltyList(elem) == Max_penalty){
          penaltyMax_out.append(elem)
        }
      }
      val v = penaltyMax_out((new Random).nextInt(penaltyMax_out.size))
      clique.remove(clique.indexOf(v))
      if(OM.isEmpty==true){
        tabuList(v) = 7
      }
      else {
        tabuList(v) = 7+(new Random).nextInt(OM.size)
      }
    }
  }
  def perturb_rand(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique:ListBuffer[Long],tabuList:Map[Long,Int],penaltyList:Map[Long,Int]): Int ={
    val OC = VertexCollect.diff(clique)
    val integer_OC = new ListBuffer[Long]//integer_oc存放可以进行替换的点
    var sign_rand = 1
    for(elem <- OC){
      var k=0//k:团中有多少个点与该点有边连接
      for(vertex <- clique){
        val temp = (elem,vertex)
        if(EdgeCollection.contains(temp)==true){
          k+=1
        }
      }
      if (k>=0.8*clique.size){
        integer_OC.append(elem)
      }
    }
    if (integer_OC.isEmpty==false){
      val OM = get_Pcandidate(VertexCollect,EdgeCollection,clique)
      val v = integer_OC((new Random).nextInt(integer_OC.size))
      val Can_Be_Delete = new ListBuffer[Long]
      for(vertex <- clique){
        val temp = (vertex,v)
        if(EdgeCollection.contains(temp)==false){
          Can_Be_Delete.append(vertex)
          if(OM.isEmpty==true){
            tabuList(vertex) = 7
          }
          else{
            tabuList(vertex) = 7+(new Random).nextInt(OM.size)
          }
        }
      }
      for (elem <- Can_Be_Delete){
        clique.remove(clique.indexOf(elem))
      }
      clique.append(v)
      penaltyList(v)+=1
    }
    else {
      sign_rand = 0
    }
    sign_rand
  }


  def Update(tabuList:Map[Long,Int]): Unit ={
    for (elem <- tabuList.keys){
      if(tabuList(elem)>0){
        tabuList(elem)-=1
      }
    }
  }

  def move_add(clique:ListBuffer[Long], PA:ListBuffer[Long], tabuList:Map[Long,Int], penaltyList:Map[Long,Int],clique_best:ListBuffer[Long]): Int ={
    var sign = 1
    val integer_add = new ListBuffer[Long]
    var minpenalty = 1000
    for (elem<- PA)
    {
      if (penaltyList(elem)<minpenalty){
        minpenalty = penaltyList(elem)
      }
    }
    for(elem <- PA){
      if (tabuList(elem) == 0 && penaltyList(elem)==minpenalty){
        integer_add.append(elem)
      }
    }
    if(integer_add.isEmpty == false){
      val v = integer_add((new Random).nextInt(integer_add.size))
      clique.append(v)
      penaltyList(v)+=1
    }
    else if(clique.length>=clique_best.length){
      val v = PA((new Random).nextInt(PA.size))
      clique.append(v)
      penaltyList(v)+=1
    }
    else
      sign = 0
    sign
  }
  //得到加点候选集
  def get_ecandidate(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique:ListBuffer[Long]): ListBuffer[Long] ={
    val ecandi_vertex = new ListBuffer[Long]
    val ecandi = VertexCollect.diff(clique)
    for(elem <- ecandi){
      var sign = 1
      for(vertex <- clique){
        val temp = (elem,vertex)
        if (EdgeCollection.contains(temp) == false){
          sign = 0
        }
      }
      if (sign==1){
        ecandi_vertex.append(elem)
      }
    }
    ecandi_vertex
  }
  //得到换点候选集
  def get_Pcandidate(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique:ListBuffer[Long]): ListBuffer[Long] ={
    val pcandi_vertex = new ListBuffer[Long]
    val pcandi = VertexCollect.diff(clique)
    for (elem <- pcandi){
      var sign = clique.size
      for (vertex <- clique){
        val temp = (vertex,elem)
        if (EdgeCollection.contains(temp)){
          sign -= 1
        }
      }
      if (sign==1){
        pcandi_vertex.append(elem)
      }
    }
    pcandi_vertex
  }

  def BLS(VertexCollect:ListBuffer[Long],EdgeCollection:ListBuffer[(Long,Long)],clique_best:ListBuffer[Long],clique:ListBuffer[Long],penaltyList:Map[Long,Int],tabuList:Map[Long,Int],counter_local:Int): ListBuffer[Long] ={

    val loop = new Breaks
    var counter_local_Pen = counter_local
    var clique_local = new ListBuffer[Long]

//    val maxsteps = 5//最大迭代次数

    val maxsteps = (1/(1+math.exp( -(VertexCollect.size+0.0)/50) )-0.5)*15

    var numsteps=0;//初始化步骤为0---------全局迭代次数

    clique.copyToBuffer(clique_local)//将当前团赋给当前的局部最优

    val initPerturb = 0.01*(VertexCollect.size)//初始化扰动强度
    val maxPerturb = 0.1*(VertexCollect.size)//最大扰动强度
    var perturbSte = initPerturb

    while(numsteps<maxsteps){
      numsteps += 1
      var PA = get_ecandidate(VertexCollect,EdgeCollection,clique)
      loop.breakable {
        while(PA.isEmpty==false){
          val sign = move_add(clique,PA,tabuList,penaltyList,clique_best)
          if (sign==0){loop.break}
          Update(tabuList)
          PA = get_ecandidate(VertexCollect,EdgeCollection,clique)
        }
      }
      if(clique.size>clique_best.size){
        clique_best.clear()
        clique.copyToBuffer(clique_best)
        counter_local_Pen = 0
      }
      else{counter_local_Pen+=1}
      if(counter_local_Pen>3){
        perturbSte = maxPerturb
        counter_local_Pen = 0
        var i = 0
        loop.breakable {
          while(i<perturbSte){
            val sign_rand = perturb_rand(VertexCollect,EdgeCollection,clique,tabuList,penaltyList)
            if(sign_rand==0){loop.break()}
            Update(tabuList)
            i=i+1
          }
        }
        clique_local.clear()
        clique.copyToBuffer(clique_local)
      }
      else{
        val P0 = 0.75
        if((clique.size==clique_local.size)&&(clique.intersect(clique_local).size == clique.size)){

          perturbSte=perturbSte+1
        }
        else {

          perturbSte=initPerturb
        }
        val p = math.max(math.exp(-(counter_local/3)),P0)
        val k = (new Random).nextFloat()
        if (k<=p){
          var i=0
          while(i<perturbSte){
            pertub_dir(VertexCollect,EdgeCollection,clique,penaltyList,tabuList, clique_best)
            Update(tabuList)
            i=i+1
          }
          clique_local.clear()
          clique.copyToBuffer(clique_local)
        }
        else{
          var i = 0
          loop.breakable {
            while(i<perturbSte){
              val sign_rand = perturb_rand(VertexCollect,EdgeCollection,clique,tabuList,penaltyList)
              if(sign_rand==0){loop.break()}
              Update(tabuList)
              i=i+1
            }
            clique_local.clear()
            clique.copyToBuffer(clique_local)
          }
        }
      }

    }
    clique_best
  }


}

