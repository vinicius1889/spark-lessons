
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkLesson1 {



  def main(args: Array[String]): Unit = {
      val sc=  new SparkContext("local[*]","SparkLesson1");
      val file = sc.textFile("src/main/resources/mussum_ipsum.txt");



      file
          .flatMap(x=>x.split("\\W+"))
            .map(_.toLowerCase)
              .filter(_.trim!="")
              .map(x=>(x,1))
              .reduceByKey((x,y)=>x+y)
              .sortBy(_._2)
              .foreach(println)

  }


}
