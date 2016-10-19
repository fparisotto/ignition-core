package ignition.core.http

import java.util.concurrent.TimeUnit

import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.language.postfixOps
import scala.util.Random

object Retry {

  sealed trait State
  case object Timeout extends State
  case object Error extends State

  def exponentialBackOff(base: Int,
                         exponent: Int,
                         initialBackoff: FiniteDuration,
                         maxBackoff: FiniteDuration,
                         maxRandom: FiniteDuration): FiniteDuration = {
    val randomMillis = maxRandom.toMillis.toInt
    val random = if (randomMillis > 0)
      FiniteDuration(Random.nextInt(randomMillis), TimeUnit.MILLISECONDS)
    else
      FiniteDuration(0, TimeUnit.MILLISECONDS)

    val calculated = scala.math.pow(base, exponent).round * (random + initialBackoff)
    calculated.min(maxBackoff)
  }

}

case class RetryConf(initialTimeoutBackoff: FiniteDuration = 100 milliseconds,
                     maxErrors: Int = 10,
                     initialBackoffOnError: FiniteDuration = 100 milliseconds,
                     timeoutMultiplicationFactor: Int = 2,
                     errorMultiplicationFactor: Int = 2,
                     maxBackoff: FiniteDuration = 1 minute,
                     maxRandom: FiniteDuration = 30 milliseconds)

case class Retry(conf: RetryConf,
                 startTime: DateTime,
                 timeout: FiniteDuration,
                 state: Retry.State = Retry.Timeout,
                 timeoutCount: Int = 0,
                 errorsCount: Int = 0) {

  import Retry._

  protected def now = DateTime.now

  private def errorBackoff =
    exponentialBackOff(conf.errorMultiplicationFactor, Math.max(errorsCount - 1, 0), conf.initialBackoffOnError, conf.maxBackoff, conf.maxRandom)
  private def timeoutBackoff =
    exponentialBackOff(conf.timeoutMultiplicationFactor, Math.max(timeoutCount - 1, 0), conf.initialTimeoutBackoff, conf.maxBackoff, conf.maxRandom)

  def onError(): Retry =
    copy(errorsCount = errorsCount + 1, state = Retry.Error)

  def onTimeout(): Retry = copy(timeoutCount = timeoutCount + 1, state = Retry.Timeout)

  def backoff(): FiniteDuration = state match {
    case Timeout => timeoutBackoff
    case Error => errorBackoff
  }

  private def canRetryMore(durations: FiniteDuration*): Boolean = {
    val maxTime = startTime.plusMillis(timeout.toMillis.toInt)
    val nextEstimatedTime = now.plusMillis(durations.map(_.toMillis.toInt).sum)
    nextEstimatedTime.isBefore(maxTime)
  }

  // This is an approximation and we are ignoring the time waiting on backoff.
  // In this way we are overestimating the average request duration, which is fine because it's better to abort early than wait too much time exceed AskTimeouts
  private def averageRequestDuration =
    Duration((now.getMillis - startTime.getMillis) / Math.max(timeoutCount + errorsCount, 1), TimeUnit.MILLISECONDS)

  def shouldGiveUp(): Boolean = state match {
    case Timeout => !canRetryMore(averageRequestDuration, timeoutBackoff)
    case Error => !canRetryMore(averageRequestDuration, errorBackoff) || errorsCount > conf.maxErrors
  }

}