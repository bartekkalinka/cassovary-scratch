import scala.io.Source

/**
 * Created by bka on 05.02.15.
 */

trait ExpReader {
  def read(path: String): (Int, Int)
}

class CurrentEmulatedReader extends ExpReader {
  private val separator = " "
  private val outEdgePattern = ("""^(\w+)""" + separator + """(\d+)""").r

  override def read(path: String) = {
    val src = Source.fromFile(path)
    val lines = src.getLines()
    var nodesCount = 0
    var edgesCount = 0

    while(lines.hasNext) {
      val outEdgePattern(id, outEdgeCount) = lines.next().trim
      val outEdgeCountInt = outEdgeCount.toInt
      nodesCount += 1
      var i = 0
      while (i < outEdgeCountInt) {
        val externalId = lines.next().trim.toInt
        i += 1
        edgesCount += 1
      }
    }
    src.close()
    (nodesCount, edgesCount)
  }
}