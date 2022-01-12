package Flink_Test_excutionEnvironment

import scala.collection.immutable

object StreamingSqlApi {
  def main(args: Array[String]): Unit = {
        var i = 100
            i += 2
        val ints: immutable.IndexedSeq[Int] = for(a <- 1 until  3)yield a*3
        val i1: Int = add(1,(x,y)=>{x-y})
        val i2: Int = add(4,test _)
        println(i1)
        println(i2)

  }
  def add(x:Int,y:(Int,Int)=>Int)={
      y(x,4)
  }
  def test(x:Int,y:Int)={
      x+y
  }
   val f=(x:Int,y:Int) =>{x/y}
}
