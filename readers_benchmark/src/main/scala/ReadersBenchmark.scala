/**
 * Created by bka on 28.01.15.
 */

import java.io.PrintWriter

import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration

import scala.util.Random

class GraphGenerator(nodesNumber: Int) {
  case class Node(number: Int, edges: Seq[Int])
  private var graph: Seq[Node] = _

  private def getOneNode(number: Int) = {
    def edgesSieve(edgesNumber: Int) = {
      for(x <- 1 to edgesNumber)
        yield Random.nextInt(nodesNumber) + 1
    }
    Node(number, edgesSieve(Random.nextInt(nodesNumber) + 1))
  }

  //TODO use streams to generate/write without storing whole graph in memory
  def generateGraphToFile(path: String): Unit = {
    val out = new PrintWriter(path , "UTF-8")

    try{
      for (i <- 1 to nodesNumber) {
        val node = getOneNode(i)
        out.println(node.number + " " + node.edges.length)
          node.edges.foreach { number =>
            out.println(s"$number")
          }
      }
    }
    finally{ out.close }
  }
}

class ReadersBenchmark(dir: String, file: String, iterations: Int) {
  def readGraph = {
    val threadPool = Executors.newFixedThreadPool(2)
    val graph = AdjacencyListGraphReader.forIntIds(dir, file,
      threadPool).toArrayBasedDirectedGraph()
    threadPool.shutdown()
    graph
  }

  def expRead = {
    val start: Long = System.currentTimeMillis

    val reader = new CurrentEmulatedReader
    val (nodesCount, edgesCount) = reader.read(dir + file)

    //TODO DRY
    val duration = Duration(System.currentTimeMillis - start, "millis")

    printf("Exp read: %s nodes and %s directed edges. Time: %s\n",
      nodesCount, edgesCount, duration)
  }

  def runOnce = {
    val start: Long = System.currentTimeMillis

    val graph = readGraph

    val duration = Duration(System.currentTimeMillis - start, "millis")

    printf("Graph: %s nodes and %s directed edges. Time: %s\n",
      graph.nodeCount, graph.edgeCount, duration)
  }

  def run = {
    for(i <- 1 to iterations) {
      //TODO parametrize
      expRead
    }
  }
}

object MainApp {
  val mainDir = "src/main/resources/"
  val mainFile = "generated_graph.txt"
  val nodesNumber = 10000

  def main(args: Array[String]) {
    def readGraph = {
      val bench = new ReadersBenchmark(mainDir, mainFile, 12).run
    }

    def generateGraph = {
      val gen = new GraphGenerator(nodesNumber)
      gen.generateGraphToFile(mainDir + mainFile)
    }

    args(0) match {
      case "parse" => readGraph
      case "gen" => generateGraph
      case "exp" => readGraph //TODO differentiate parse and exp
    }
  }
}