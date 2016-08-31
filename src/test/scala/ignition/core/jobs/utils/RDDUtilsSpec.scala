package ignition.core.jobs.utils

import ignition.core.testsupport.spark.SharedSparkContext
import ignition.core.jobs.utils.RDDUtils._
import org.scalatest._

import scala.util.Random

class RDDUtilsSpec extends FlatSpec with Matchers with SharedSparkContext {

  "RDDUtils" should "provide groupByKeyAndTake" in {
    (10 to 60 by 10).foreach { take =>
      val rdd = sc.parallelize((1 to 400).map(x => "a" -> Random.nextInt()) ++ (1 to 400).map(x => "b" -> Random.nextInt()), 60)
      val result = rdd.groupByKeyAndTake(take).collect().toMap
      result("a").length shouldBe take
      result("b").length shouldBe take
    }
  }

  it should "provide groupByKeyAndTakeOrdered" in {
    val take = 50
    val aList = (1 to Random.nextInt(400) + 100).map(x => "a" -> Random.nextInt()).toList
    val bList = (1 to Random.nextInt(400) + 100).map(x => "b" -> Random.nextInt()).toList
    val rdd = sc.parallelize(aList ++ bList)
    val result = rdd.groupByKeyAndTakeOrdered(take).collect().toMap
    result("a") shouldBe aList.map(_._2).sorted.take(take)
    result("b") shouldBe bList.map(_._2).sorted.take(take)
  }

}
