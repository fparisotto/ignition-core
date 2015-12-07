package ignition.core.utils

import ignition.core.utils.ExceptionUtils._
// Used mainly to augment scalacheck traces in scalatest
trait BetterTrace {
  def fail(message: String): Nothing
  def withBetterTrace(block: => Unit): Unit =
    try {
      block
    } catch {
      case t: Throwable => fail(s"${t.getMessage}: ${t.getFullStackTraceString}")
    }

}
