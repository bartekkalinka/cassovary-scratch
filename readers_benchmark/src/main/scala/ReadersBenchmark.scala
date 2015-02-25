/**
 * Created by bka on 28.01.15.
 */

import java.io.PrintWriter

import scala.concurrent.duration.Duration

import scala.util.Random

class GraphGenerator(nodesNumber: Int, edgesProbability: Double) {
  case class Node(number: Int, edges: Seq[Int])
  private var graph: Seq[Node] = _

  private def getOneNode(number: Int) = {
    Node(number, for(x <- 1 to (nodesNumber * edgesProbability).toInt) yield Random.nextInt(nodesNumber) + 1)
  }

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

  def generateIntList(path: String): Unit = {
    val out = new PrintWriter(path , "UTF-8")
    val intNumber = nodesNumber * nodesNumber / 2

    try{
      for (i <- 1 to intNumber) {
        val number = Random.nextInt(nodesNumber)
        out.println(s"$number")
      }
    }
    finally{ out.close }
  }
}

class ReadersBenchmark(reader: ExpReader, dir: String, file: String, iterations: Int) {
  def runOnce = {
    val start: Long = System.currentTimeMillis
    val (nodeCount, edgeCount) = reader.read(dir, file)
    val duration = Duration(System.currentTimeMillis - start, "millis")
    printf("Graph: %s nodes and %s directed edges. Time: %s\n",
      nodeCount, edgeCount, duration)
  }

  def run = {
    for(i <- 1 to iterations) {
      runOnce
    }
  }
}

object MainApp {
  val mainDir = "src/main/resources/"
  val mainFile = "generated_graph.txt"
  val nodesNumber = 10000
  val edgesProbability = 0.5
  val iterations = 12

  def main(args: Array[String]) {
    def readGraph(reader: ExpReader) = {
      val bench = new ReadersBenchmark(reader, mainDir, mainFile, iterations).run
    }

    def generateGraph = {
      val gen = new GraphGenerator(nodesNumber, edgesProbability)
      gen.generateGraphToFile(mainDir + mainFile)
    }

    args(0) match {
      //full adjacency list format
      case "gen" => generateGraph
      case "parse" => readGraph(new CurrentRealReader)
      case "emul" => readGraph(new CurrentEmulatedReader)
      case "exp" => readGraph(new InputStreamBasedReader)

      //just list of integers
      case "ints" => new GraphGenerator(nodesNumber, edgesProbability).generateIntList(mainDir + mainFile)
      case "parseints1" => readGraph(new IntsReader1)
      case "parseints2" => readGraph(new IntsReader2)
      case "parseints3" => readGraph(new IntsReader3)
      case "parseints4" => readGraph(new IntsReader4)
      case "parseints5" => readGraph(new IntsReader5)
    }
  }
}