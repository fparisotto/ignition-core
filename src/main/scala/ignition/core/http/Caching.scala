package ignition.core.http

import org.slf4j.LoggerFactory
import spray.caching.Cache

import scala.concurrent._
import scala.util.Failure

trait Caching[T] {
  val log = LoggerFactory.getLogger(classOf[Caching[T]])

  val cache: Cache[T]
  import ExecutionContext.Implicits.global
  def fetchCache[K](key: K, f: () => Future[T]): Future[T] = cache(key) {
    f.apply andThen {
      case Failure(e) => {
        cache.remove(key)
        log.info(s"Removed $key from cache due to an exception: $e")
      }
    }
  }
}
