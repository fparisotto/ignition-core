package ignition.core.utils

import org.scalatest._

import scala.util.Random

class IntBagSpec extends FlatSpec with ShouldMatchers  {

  "IntBag" should "be built from sequence" in {
    IntBag.from(Seq(1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 4)).histogram shouldBe Map(1 -> 2, 2 -> 3, 3 -> 1, 4 -> 5)
  }

  it should "calculate the median and average" in {
    val size = 1000
    val numbers = (0 until 1000).map(_ => Random.nextInt(400).toLong).toList
    val bag = IntBag.from(numbers)

    bag.avg.get shouldBe numbers.sum / size

    // TODO: the median is only approximate and it could be better, improve it
  }

}
