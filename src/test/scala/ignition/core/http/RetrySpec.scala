package ignition.core.http

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class RetrySpec extends FlatSpec with Matchers {
  "Retry" should "return the initial backoff" in {
    val now = DateTime.now
    val timeout = 60.seconds

    val retry = Retry(RetryConf(initialBackoffOnError = 123.milliseconds, initialTimeoutBackoff = 456.milliseconds, maxRandom = 0.seconds), now, timeout)

    retry.onError().backoff() shouldBe 123.millisecond
    retry.onTimeout().backoff() shouldBe 456.millisecond
  }

  it should "multiply by the factor on second time" in {

    val now = DateTime.now
    val timeout = 60.seconds

    val retry = Retry(RetryConf(initialBackoffOnError = 123.milliseconds, initialTimeoutBackoff = 456.milliseconds, maxRandom = 0.seconds, timeoutMultiplicationFactor = 3, errorMultiplicationFactor = 5), now, timeout)

    retry.onError().onError().backoff() shouldBe (123 * 5).millisecond
    retry.onTimeout().onTimeout().backoff() shouldBe (456 * 3).millisecond
  }

  it should "not explode if called with no errors or timeouts" in {
    val now = DateTime.now
    val timeout = 60.seconds

    val retry = Retry(RetryConf(maxRandom = 0.seconds), now, timeout)

    retry.backoff() shouldBe 100.milliseconds
  }

}
