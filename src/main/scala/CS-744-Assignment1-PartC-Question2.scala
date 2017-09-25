import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.RangePartitioner

object PageRankPartC2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CS-744-Assignment1-PartC-2")

    val sc = new SparkContext(conf)

    val datafile = sc.textFile("/spark/deployment/web-BerkStan.txt", 20)

    val raw_data = datafile.filter(f => !f.startsWith("#"))
    // raw_data.count

    val data = raw_data.map(line => line.split("\\s+"))

    // Remove partial data and self loops
    val filtered_data = data.filter(e => (e.size == 2 && !e(0).equals(e(1))))
    // filtered_data.count

    // val datatuples = filtered_data.map(e => (e(0), e(1)))
    // TODO: Partition
    val datatuples = filtered_data.map(e => (e(0), List(e(1))))
    // datatuples.count
    // val rangePartitioner = new RangePartitioner(15, datatuples)

    var init_page_ranks = filtered_data.flatMap(e => List(e(0), e(1))).distinct.map(f => (f, 1.0))
    
    val rankPartitioner = new RangePartitioner(20, datatuples)
    init_page_ranks = init_page_ranks.partitionBy(rankPartitioner)
    var page_ranks = init_page_ranks
    // page_ranks.count
    
    val partitioned_datatuples = datatuples.partitionBy(rankPartitioner)

    val groupedData = partitioned_datatuples.reduceByKey((a,b) => a.:::(b))

    def generateContrib(rank: Double, neighbours: Option[List[String]]) = {
       val new_rank: Double = neighbours match {
         case Some(value) => 1.0*rank/neighbours.get.size
         case None => 0.0
       }
       neighbours.getOrElse(List()).map(n => (n, new_rank))
    }

    // def generateContrib(rank: Double, neighbours: List[String]) = {
    //     val new_rank: Double = 1.0*rank/neighbours.size
    //     neighbours.map(n => (n, new_rank))
    // }

    // Todo: Partition
    val exploded_contribs = page_ranks.leftOuterJoin(groupedData).flatMap(e => generateContrib(e._2._1, e._2._2))
    // val exploded_contribs = groupedData.join(page_ranks).flatMap(e => generateContrib(e._2._2, e._2._1))
    // val exploded_contribs = page_ranks.join(groupedData).flatMap(e => generateContrib(e._2._1, e._2._2))
    
    page_ranks = exploded_contribs.reduceByKey((a,b) => (a+b)).mapValues(v => (0.15 + 0.85*v))

    for (x <- 1 until 10) {
      // val exploded_contribs = groupedData.join(page_ranks).flatMap(e => generateContrib(e._2._2, e._2._1))
      // TODO: Partition
      val exploded_contribs = page_ranks.leftOuterJoin(groupedData).flatMap(e => generateContrib(e._2._1, e._2._2))
      // val exploded_contribs = page_ranks.join(groupedData).flatMap(e => generateContrib(e._2._1, e._2._2))
      page_ranks = exploded_contribs.reduceByKey(_+_).mapValues(v => 0.15 + 0.85*v)
      
    }

    // page_ranks.count

    val final_page_ranks = init_page_ranks.leftOuterJoin(page_ranks).map(e => {
      val rank = e._2._2 match {
        case Some(value) => value
        case None => e._2._1
      }

      (e._1, rank)
    })

    final_page_ranks.count
    final_page_ranks.take(100).foreach(println)
    sc.stop()
    // Thread.sleep(20000)
  }
}
