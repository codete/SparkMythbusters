import java.io.InputStream

import org.scalatest.{BeforeAndAfter, FunSuite}
import App._
import org.apache.spark.{SparkConf, SparkContext}

class App$Test extends FunSuite with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "testApp"

  private var sparkContext: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
  }

  test("countWordsNormal") {
    val start = System.currentTimeMillis()
    val lines = readFileLines("words.txt")
    val result = countWordsNormal(lines)
    val stop = System.currentTimeMillis()

    println(stop-start)

    assert(result._1 === "the")
  }

  test("countWordsSpark") {
    val start = System.currentTimeMillis()
    val lines = readFileLines("words.txt")
    val linesRdd = sparkContext.parallelize(lines)

    val result = countWordsSpark(linesRdd)
    val stop = System.currentTimeMillis()

    println(stop-start)

    assert(result._1 === "the")
  }

  def readFileLines(filename: String) : List[String] = {
    val stream: InputStream = getClass.getResourceAsStream(filename)
    scala.io.Source.fromInputStream(stream).getLines().toList
  }

}
