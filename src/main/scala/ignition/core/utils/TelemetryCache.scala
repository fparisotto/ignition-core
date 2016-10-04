package ignition.core.utils

import ignition.core.utils.TelemetryCache.TelemetryCacheReporter
import spray.caching.Cache

import scala.concurrent.{ExecutionContext, Future}

object TelemetryCache {

  def apply[V](cacheName: String, wrapped: Cache[V], reporter: TelemetryCacheReporter): Cache[V] =
    new TelemetryCache[V](cacheName, wrapped, reporter)

  trait TelemetryCacheReporter {
    def onHit(name: String): Unit
    def onMiss(name: String): Unit
  }

}

class TelemetryCache[V](cacheName: String, wrapped: Cache[V], reporter: TelemetryCacheReporter) extends Cache[V] {

  override def apply(key: Any, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[V] = {
    val value = wrapped.get(key)
    if (value.isDefined) {
      reporter.onHit(cacheName)
      value.get
    } else {
      reporter.onMiss(cacheName)
      wrapped.apply(key, genValue)
    }
  }

  override def get(key: Any): Option[Future[V]] = wrapped.get(key)

  override def clear(): Unit = wrapped.clear()

  override def size: Int = wrapped.size

  override def remove(key: Any): Option[Future[V]] = wrapped.remove(key)

  override def keys: Set[Any] = wrapped.keys

  override def ascendingKeys(limit: Option[Int]): Iterator[Any] = wrapped.ascendingKeys(limit)

}
