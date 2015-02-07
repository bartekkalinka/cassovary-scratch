import java.io.{DataInputStream, BufferedReader, InputStreamReader, FileInputStream}
import java.util.concurrent.Executors

import com.twitter.cassovary.util.io.AdjacencyListGraphReader

import scala.io.Source

/**
 * Created by bka on 05.02.15.
 */

trait ExpReader {
  def read(dir: String, file: String): (Int, Long)
}

class CurrentRealReader extends ExpReader {
  override def read(dir: String, file: String) = {
    val threadPool = Executors.newFixedThreadPool(2)
    val graph = AdjacencyListGraphReader.forIntIds(dir, file,
      threadPool).toArrayBasedDirectedGraph()
    threadPool.shutdown()
    (graph.nodeCount, graph.edgeCount)
  }
}

class CurrentEmulatedReader extends ExpReader {
  private val separator = " "
  private val outEdgePattern = ("""^(\w+)""" + separator + """(\d+)""").r

  override def read(dir: String, file: String) = {
    val src = Source.fromFile(dir + file)
    val lines = src.getLines()
    var nodeCount = 0
    var edgeCount = 0L

    while(lines.hasNext) {
      val outEdgePattern(id, outEdgeCount) = lines.next().trim
      val outEdgeCountInt = outEdgeCount.toInt
      nodeCount += 1
      var i = 0
      while (i < outEdgeCountInt) {
        val externalId = lines.next().trim.toInt
        i += 1
        edgeCount += 1
      }
    }
    src.close()
    (nodeCount, edgeCount)
  }
}

class InputStreamBasedReader extends ExpReader {
  private val separator = ' '

  override def read(dir: String, file: String) = {
    var nodeCount = 0
    var edgeCount = 0L
    val stream = new FileInputStream(dir + file)
    val reader = new BufferedReader(new InputStreamReader(stream))
    var line = reader.readLine()
    while(line != null) {
      val Array(id, outEdgeCount) = line.split(separator).map(_.toInt)
      nodeCount += 1
      var i = 0
      while (i < outEdgeCount) {
        val externalId = reader.readLine().toInt
        i += 1
        edgeCount += 1
      }
      line = reader.readLine()
    }
    stream.close()
    reader.close()
    (nodeCount, edgeCount)
  }
}

