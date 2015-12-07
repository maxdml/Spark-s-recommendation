import scala.math._

object Cartesian {
  def main() {
    //var combinations = scala.collection.immutable.IndexedSeq[String]()
    var combinations = scala.collection.immutable.IndexedSeq[((Int, Int), (Int,Int,Int))]()
    val cluster = scala.collection.immutable.Seq[Node](new Node(8, 14, 29))
    //                                                   new Node(8, 6, 1))
    //                                                   new Node(8, 10, 7))
    cluster.foreach { node =>
      combinations =
          (1 to node.memory).flatMap(m =>
            (1 to node.cores).map(c =>// {
//              val exec: Int = min(floor(node.memory/m), floor(node.cores/c)).toInt * node.number
//              s"${exec.toString}-${m.toString}-${c.toString}"
//            }))
              (m, c) -> (min(floor(node.memory/m), floor(node.cores/c)).toInt * node.number, m * node.number , c * node.number)))
    }
  
    combinations.foreach(println)
  }
}

case class Node(cores: Int, memory: Int, number: Int)

Cartesian.main
