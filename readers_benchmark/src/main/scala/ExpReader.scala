import java.io._
import java.util.Scanner
import java.util.concurrent.Executors
import java.nio.channels.FileChannel.MapMode._
import java.nio.ByteOrder._

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
        val line = reader.readLine()
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

class IntsReader1 extends ExpReader {
  override def read(dir: String, file: String) = {
    var edgeCount = 0L
    val stream = new FileInputStream(dir + file)
    val reader = new BufferedReader(new InputStreamReader(stream))
    var line = reader.readLine()
    while(line != null) {
      //val externalId = line.toInt
      edgeCount += 1
      line = reader.readLine()
    }
    stream.close()
    reader.close()
    (0, edgeCount)
  }
}

class IntsReader2 extends ExpReader {
  private val separator = '\n'

  //TODO draft - calculates edge count incorrectly
  override def read(dir: String, file: String) = {
    val stream = new FileInputStream(dir + file)
    val chunkSize = 100000
    val data = new Array[Byte](chunkSize)
    var bytesRead = stream.read(data)
    var edgeCount = 0L
    var remainder = ""

    while(bytesRead != -1) {
      val str =
        if(bytesRead == chunkSize) {
          remainder +  new String(data, "ASCII")
        }
        else {
          remainder + new String(data.take(bytesRead), "ASCII")
        }
      val reader = new BufferedReader(new StringReader(str))
      var line = reader.readLine()
      while(line != null) {
        //val externalId = line.toInt
        edgeCount += 1
        remainder = line + "\n"
        line = reader.readLine()
      }
      bytesRead = stream.read(data)
      if(bytesRead != -1) {
        edgeCount -= 1
      }
    }
    stream.close()
    (0, edgeCount)
  }
}

class IntsReader3 extends ExpReader {
  private val separator = '\n'
  private val chunkSize = 1000000

  //TODO total draft - doesn't really calculate edge count
  override def read(dir: String, inputFile: String) = {
    var edgeCount = 0L
    val file = new File("" + dir + inputFile)
    val fileSize = file.length.toInt
    val stream = new FileInputStream(file)
    val buffer = stream.getChannel.map(READ_ONLY, 0, fileSize)
    val data = new Array[Byte](chunkSize)
    buffer.order(LITTLE_ENDIAN)

    while(buffer.hasRemaining) {
      var i = 0
      while(buffer.hasRemaining && i < chunkSize) {
        data(i) = buffer.get
        i += 1
      }
      val str = new String(data, "ASCII")
      val reader = new BufferedReader(new StringReader(str))
      var line = reader.readLine()
      while(line != null) {
        //val externalId = line.toInt
        edgeCount += 1
        line = reader.readLine()
      }
    }

    stream.close()
    (0, edgeCount)
  }
}

class IntsReader4 extends ExpReader {
  override def read(dir: String, file: String) = {
    var edgeCount = 0L
    val stream = new FileInputStream(dir + file)
    val scan = new Scanner(stream)
    var line = scan.nextLine()
    while(line != null) {
      //val externalId = line.toInt
      edgeCount += 1
      line = scan.nextLine()
    }
    stream.close()
    (0, edgeCount)
  }
}
