package ignition.core.utils

import scala.util.Try
import scala.util.control.NonFatal

object AutoCloseableIterator {
  case object empty extends AutoCloseableIterator[Nothing] {
    override def naiveHasNext() = false
    override def naiveNext() = throw new Exception("Empty AutoCloseableIterator")
    override def naiveClose() = {}
  }

  def wrap[T](iterator: Iterator[T], doClose: () => Unit = () => ()): AutoCloseableIterator[T] = new AutoCloseableIterator[T] {
    override def naiveClose(): Unit = doClose()
    override def naiveHasNext(): Boolean = iterator.hasNext
    override def naiveNext(): T = iterator.next()
  }
}

trait AutoCloseableIterator[T] extends Iterator[T] with AutoCloseable {
  // Naive functions should be implemented by the user as in a standard Iterator/AutoCloseable
  def naiveHasNext(): Boolean
  def naiveNext(): T
  def naiveClose(): Unit

  var closed = false

  // hasNext closes the iterator and handles the case where it is already closed
  override def hasNext: Boolean = if (closed)
    false
  else {
    val naiveResult = try {
      naiveHasNext
    } catch {
      case NonFatal(e) =>
        Try { close }
        throw e
    }
    if (naiveResult)
      true
    else {
      close // auto close when exhausted
      false
    }
  }

  // next closes the iterator and handles the case where it is already closed
  override def next(): T = if (closed)
    throw new RuntimeException("Trying to get next element on a closed iterator")
  else if (hasNext)
    try {
      naiveNext
    } catch {
      case NonFatal(e) =>
        Try { close }
        throw e
    }
  else
    throw new RuntimeException("Trying to get next element on an exhausted iterator")

  override def close() = if (!closed) {
    closed = true
    naiveClose
  }

  override def finalize() = Try { close }
}
