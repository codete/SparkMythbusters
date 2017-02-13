import org.apache.spark.rdd.RDD

object App {

  def countWordsSpark(lines: RDD[String]) : (String, Int) = {
    val sortedWordCount = lines.
      flatMap( line => line.split(" ")).
      filter( word => !word.trim.isEmpty).
      map( word => (word.toLowerCase(),1)).
      reduceByKey(_ + _).
      sortBy(-_._2).collect().toList

    sortedWordCount.head
  }

  def countWordsNormal(lines: List[String]) : (String, Int) = {
    val sortedWordCount = lines.
      flatMap( line => line.split(" ")).
      filterNot(word => word.trim().isEmpty).
      groupBy( word => word.toLowerCase()).
      map(group => (group._1, group._2.length)).
      toList.sortBy(-_._2)

    sortedWordCount.head
  }

}
