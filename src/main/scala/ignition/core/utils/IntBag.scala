package ignition.core.utils

import ignition.core.utils.CollectionUtils._

object IntBag {
  def from(numbers: TraversableOnce[Long]): IntBag = {
    val histogram = scala.collection.mutable.HashMap.empty[Long, Long]
    numbers.foreach(n => histogram += (n -> (histogram.getOrElse(n, 0L) + 1)))
    IntBag(histogram)
  }

  val empty = from(Seq.empty)
}

case class IntBag(histogram: collection.Map[Long, Long]) {
  def ++(other: IntBag): IntBag = {
    val newHistogram = scala.collection.mutable.HashMap.empty[Long, Long]
    (histogram.keySet ++ other.histogram.keySet).foreach(k => newHistogram += (k -> (histogram.getOrElse(k, 0L) + other.histogram.getOrElse(k, 0L))))
    new IntBag(newHistogram)
  }


  def median: Option[Long] = {
    percentile(50)
  }

  def percentile(n: Double): Option[Long] = {
    require(n > 0 && n <= 100)
    histogram.keys.maxOption.flatMap { max =>
      val total = histogram.values.sum
      val position = total * (n / 100)

      val accumulatedFrequency = (0L to max).scanLeft(0L) { case (sumFreq, k) => sumFreq + histogram.getOrElse(k, 0L) }.zipWithIndex
      accumulatedFrequency.collectFirst { case (sum, k) if sum >= position => k - 1 }
    }
  }

  def avg: Option[Long] = {
    if (histogram.nonEmpty) {
      val sum = histogram.map { case (k, f) => k * f }.sum
      val count = histogram.values.sum
      Option(sum / count)
    } else
      None
  }

  def min: Option[Long] = {
    histogram.keys.minOption
  }

  def max: Option[Long] = {
    histogram.keys.maxOption
  }
}
