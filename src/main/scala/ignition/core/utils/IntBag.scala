package ignition.core.utils

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
    if (histogram.nonEmpty) {
      val total = histogram.values.sum
      val half = total / 2
      val max = histogram.keys.max

      val accumulatedFrequency = (0L to max).scanLeft(0L) { case (sumFreq, k) => sumFreq + histogram.getOrElse(k, 0L) }.zipWithIndex
      accumulatedFrequency.collectFirst { case (sum, k) if sum >= half => k }
    } else {
      None
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
}
