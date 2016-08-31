package ignition.core.utils

import ignition.core.utils.ExceptionUtils._
import org.scalactic.source
// Used mainly to augment scalacheck traces in scalatest
trait BetterTrace {
  def fail(message: String)(implicit pos: source.Position): Nothing

  def withBetterTrace(block: => Unit): Unit =
    try {
      block
    } catch {
      case t: Throwable => fail(s"${t.getMessage}: ${t.getFullStackTraceString}")
    }

}
