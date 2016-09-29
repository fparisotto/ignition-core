package ignition.core.http

import java.util.Random
import java.util.concurrent.TimeUnit

import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.language.postfixOps

object Retry {

  sealed trait State
  case object Timeout extends State
  case object Error extends State

  val random = new Random

  val _maxWaitForNextRetry = 10

  def exponentialBackOff(r: Int): FiniteDuration = {
    val exponent: Double = scala.math.min(r, _maxWaitForNextRetry)
    scala.math.pow(2, exponent).round * (random.nextInt(30) + 100 milliseconds)
  }

}

case class Retry(startTime: DateTime,
                 timeout: FiniteDuration,
                 state: Retry.State = Retry.Timeout,
                 timeoutCount: Int = 0,
                 timeoutBackoff: FiniteDuration = 100 milliseconds,
                 maxErrors: Int = 10,
                 errorsCount: Int = 0,
                 backoffOnError: FiniteDuration = 100 milliseconds) {

  import Retry._

  def onError(): Retry =
    copy(errorsCount = errorsCount + 1, backoffOnError = exponentialBackOff(errorsCount + 1), state = Retry.Error)

  def onTimeout(): Retry = copy(timeoutCount = timeoutCount + 1, timeoutBackoff = exponentialBackOff(timeoutCount + 1), state = Retry.Timeout)

  def backoff(): FiniteDuration = state match {
    case Timeout => timeoutBackoff
    case Error => backoffOnError
  }

  private def canRetryMore(durations: FiniteDuration*): Boolean = {
    val maxTime = startTime.plusMillis(timeout.toMillis.toInt)
    val nextEstimatedTime = DateTime.now.plusMillis(durations.map(_.toMillis.toInt).sum)
    nextEstimatedTime.isBefore(maxTime)
  }

  // This is an approximation and we are ignoring the time waiting on backoff.
  // In this way we are overestimating the average request duration, which is fine because it's better to abort early than wait too much time exceed AskTimeouts
  private def averageRequestDuration =
    Duration((DateTime.now.getMillis - startTime.getMillis) / Math.max(timeoutCount + errorsCount, 1), TimeUnit.MILLISECONDS)

  def shouldGiveUp(): Boolean = state match {
    case Timeout => !canRetryMore(averageRequestDuration, timeoutBackoff)
    case Error => !canRetryMore(averageRequestDuration, backoffOnError) || errorsCount > maxErrors
  }

}