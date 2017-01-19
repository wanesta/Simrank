/**
  * Created by gaoshuming on 29/12/2016
  **/
package com.autohome.algo.simrank
import breeze.linalg.{Vector => BV}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class Matrix() {
  def apply(rowCount: Int, colCount: Int)(f: (Int, Int) => Double) = (
    for (i <- 1 to rowCount) yield
      (for (j <- 1 to colCount) yield f(i, j)).toList
    ) //.toList
}
class Simranktest() {

  def graphstruct(nods: RDD[(String, String)]): Graph[String, String] = {
    val indnode = (nods.map(_._1) union (nods.map(_._2))).distinct.zipWithIndex() //增加索引的节点集合且去重
    //val relationships: RDD[Edge[String]]  = nods.join(indnode).map(x=>x._2).join(indnode).map(_._2).map { x =>
    val relationships = nods.map { x =>
      val x1 = (x._1 + "L").replace("L", "").toLong
      val x2 = (x._2 + "L").replace("L", "").toLong
      val ed = Edge(x1, x2, "1")
      ed
    }.groupBy { x => (x.srcId, x.dstId) }.map { x =>
      val set = Edge(x._1._1.toString.toLong, x._1._2.toString.toLong, x._2.toVector.length.toString)
      set
    }
    val users = indnode.map(line => (line, "id")).map { x =>
      val x1 = x._1._2
      (x1, x._2)
    }
    // Define a default user in case there are relationship with missing user
    // Build the initial Graph with nodes and edge
    val g = Graph(users, relationships)
    g
  }
}
object Simrank {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val delta = 0.8

    val conf = new SparkConf().setAppName("AD_Sim")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val broadcastnode = sc.broadcast()

    val file = sc.textFile(args(0))     //  /Users/gaoshuming/Downloads/data/1.txt
    val nods: RDD[(String, String)] = file.map{ x =>
      val xset = (x.split("\t")(0),x.split("\t")(1))
      xset
    }

    val indnodeall = (nods.map(_._1)union(nods.map(_._2))).distinct.zipWithIndex() //增加索引的节点集合且去重
    var len = indnodeall.collect.length
    //val indexs: RDD[(VertexId, VertexId)] = nods.join(indnodeall).map(x=>x._2).join(indnodeall).map(_._2)  //convert to the index 转换成索引

    val graphs = new Simranktest()
    val gra = graphs.graphstruct(nods)
    val outs = gra.outDegrees.map(x => (x._1,(1.toDouble / x._2.toDouble)))
    val ins = gra.inDegrees.map(x => (x._1,(1.toDouble / x._2.toDouble)))
    val gras = gra.triplets.filter(_.attr.toInt != 1)
    val stgra = gra.outerJoinVertices(outs)((id, _, degin) => (id.toString,degin.getOrElse(0))).triplets.map{x =>
      val set = (x.srcId,x.dstId,x.srcAttr._2.toString.toDouble * x.attr.toInt)
      //val st = (x.dstId,x.srcId,(1.toDouble / len))//设置为0时，分布式矩阵自动压缩成行列不相等的矩阵(非方阵)
      set
    }
    val testg = gra.outerJoinVertices(outs)((id, _, degin) => (id.toString,degin.getOrElse(0))).triplets.filter(_.attr.toInt != 1).map{x =>
      val set = (x.srcId,x.dstId,x.srcAttr._2.toString.toDouble, x.attr.toInt + 1)
      //val st = (x.dstId,x.srcId,(1.toDouble / len))//设置为0时，分布式矩阵自动压缩成行列不相等的矩阵(非方阵)
      set
    }
    //testg.foreach(println)
    //val sg = stgra//.map(_._1).union(stgra.map(_._2))
    val indnode = (stgra.map(_._1).union(stgra.map(_._2))).distinct.zipWithIndex()
    val sgnu = stgra.map(x=>(x._1,(x._2,x._3))).join(indnode).map(x=>(x._2._1._1,(x._2._1._2,x._2._2))).join(indnode).map(x=>(x._2._1._2,x._2._2,x._2._1._1))
    sgnu.foreach(println)
    val sg = sgnu.map(x=> (x._2,x._1,x._3)).union(sgnu.map(x=>(x._1,x._2,1.toDouble / len)))

    len = sg.count.toInt
    val inputGraph: Graph[PartitionID, String] = gra.outerJoinVertices(gra.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank
    val node_len= gra.triplets.map(trip => trip.srcId).union(gra.triplets.map(trip => trip.dstId)).distinct.count.toInt //获取所有节点总数

    val m_list = new Matrix() //生成矩阵格式的List

    //val node1 = gra.edges.map(x => x.relativeDirection(EdgeDirection.Either)).collect
    //all the input neighbors
    val maptab = gra.collectNeighborIds(EdgeDirection.Either).map{ x =>
      val set = (x._1,x._2.toVector,1.toDouble/x._2.length.toDouble)
      set
    }

    val matrixprob = new CoordinateMatrix(sg.map{ x =>     //概率转移矩阵
      val s = MatrixEntry(x._1,x._2,x._3)
      s
    })

    len = matrixprob.numRows.toInt
    val matrixp = new CoordinateMatrix(matrixprob.entries.map{x =>
      val set = MatrixEntry(x.i,x.i,1.0)
      set
    })
    matrixprob.entries.foreach(println)
    val unit = new CoordinateMatrix(matrixp.entries.map{x=> (x.i,x.i,x.value)}.union(matrixp.entries.map(x=>(x.j,x.j,x.value))).map{ x=>
      val set = MatrixEntry(x._1,x._2,x._3 * delta)
      set
    }.distinct)
    val In = new CoordinateMatrix(matrixp.entries.map{x => //s0 = (1-C)In
      val set = MatrixEntry(x.i,x.j,x.value * (1 - delta))
      set
    })

    val S0 = In   //初始化S0
    var Sk = S0
    var Skp1 = Sk

    //var mx= Skp1.toBlockMatrix.subtract(Sk.toBlockMatrix()).toCoordinateMatrix.entries.map(_.value.abs).max
    for(i <- 0 to 10){//mx >= math.pow(delta,k)) {
      //println( "Ck -> "+"%.10f" format math.pow(delta,k))
      Skp1 = new CoordinateMatrix(Sk.toBlockMatrix.multiply(matrixprob.toBlockMatrix.transpose).multiply(matrixprob.toBlockMatrix).toCoordinateMatrix.entries.map{x =>
        val set = MatrixEntry(x.i,x.j,x.value * delta)
        set
      }).toBlockMatrix.add(In.toBlockMatrix).toCoordinateMatrix()
      //mx = Skp1.toBlockMatrix.subtract(Sk.toBlockMatrix()).toCoordinateMatrix.entries.map(_.value.abs).max
      Sk = Skp1
      //println("MAX -> " + "%.10f" format mx)
    }
    val result =Skp1.entries.map{
      case MatrixEntry(x,y,j) => (x,y,"%4.3f" format j)
    }.map(x => (x._1,(x._2,x._3))).join(indnode.map(x=>(x._2,x._1.toLong)))
      .map(x=>(x._2._1._1,(x._2._1._2,x._2._2))).join(indnode.map(x=>(x._2,x._1.toLong)))
      .map(x => (x._2._1._2,x._2._2,x._2._1._1))

    result.filter(_._3.toDouble < 1.0).saveAsTextFile("/output")
  }
}
