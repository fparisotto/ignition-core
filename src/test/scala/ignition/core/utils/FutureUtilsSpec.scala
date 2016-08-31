package ignition.core.utils
import FutureUtils._
import org.scalatest._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class FutureUtilsSpec extends FlatSpec with Matchers {
  "FutureUtils" should "provide toLazyIterable" in {
    val timesCalled = collection.mutable.Map.empty[Int, Int].withDefaultValue(0)

    val generators = (0 until 20).map { i => () => Future { timesCalled(i) += 1 ; i } }
    val iterable = generators.toLazyIterable()
    val iterator = iterable.toIterator
    timesCalled.forall { case (key, count) => count == 0 } shouldBe true

    Await.result(iterator.next(), 2.seconds)

    timesCalled(0) shouldBe 1

    (1 until 20).foreach { i => timesCalled(i) shouldBe 0 }

    Await.result(Future.sequence(iterator), 5.seconds).toList shouldBe (1 until 20).toList

    (0 until 20).foreach { i => timesCalled(i) shouldBe 1 }
  }

  it should "provide collectAndTake" in {
    val timesCalled = collection.mutable.Map.empty[Int, Int].withDefaultValue(0)
    val iterable = (0 until 30).map { i => () => Future { timesCalled(i) += 1 ; i } }.toLazyIterable()

    val expectedRange = Range(5, 15)
    val result = Await.result(iterable.collectAndTake({ case i if expectedRange.contains(i) => i }, n = expectedRange.size), 5.seconds)

    result shouldBe expectedRange.toList

    (0 until 20).foreach { i => timesCalled(i) shouldBe 1 } // 2 batches of size 10
    (20 until 30).foreach { i => timesCalled(i) shouldBe 0 } // last batch won't be ran

  }

}
