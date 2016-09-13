package ignition.core.utils

import org.scalatest._

import scala.util.Random

class IntBagSpec extends FlatSpec with Matchers  {

  "IntBag" should "be built from sequence" in {
    IntBag.from(Seq(1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 4)).histogram shouldBe Map(1 -> 2, 2 -> 3, 3 -> 1, 4 -> 5)
  }

  it should "calculate the average" in {
    val size = 1000
    val numbers = (0 until size).map(_ => Random.nextInt(400).toLong).toList
    val bag = IntBag.from(numbers)

    bag.avg.get shouldBe numbers.sum / size
  }

  it should "calculate the percentile, min and max" in {
    val size = 3 // anything different is hard to guess because of the approximation
    val numbers = (0 until size).map(_ => Random.nextInt(400).toLong).toList
    val bag = IntBag.from(numbers)

    bag.min.get shouldBe numbers.min
    bag.percentile(0.1).get shouldBe numbers.min
    bag.median.get shouldBe numbers.sorted.apply(1)
    bag.percentile(99.9).get shouldBe numbers.max
    bag.max.get shouldBe numbers.max
  }

}
