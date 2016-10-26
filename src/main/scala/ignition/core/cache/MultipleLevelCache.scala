package ignition.core.cache

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import ignition.core.utils.FutureUtils._
import org.joda.time.{DateTime, Period}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}


object ExpiringMultipleLevelCache {
  case class TimestampedValue[V](date: DateTime, value: V) {
    def hasExpired(ttl: Period, now: DateTime): Boolean = {
      date.plus(ttl).isBefore(now)
    }
  }

  trait GenericCache[V] {
    def apply(key: String, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V]
  }

  trait LocalCache[V] extends GenericCache[V] {
    def get(key: Any): Option[Future[V]]
    def set(key: Any, value: Try[V]): Unit
  }

  trait RemoteWritableCache[V] {
    def set(key: String, value: V)(implicit ec: ExecutionContext): Future[Unit]
    def setLock(key: String, ttl: FiniteDuration): Future[Boolean]
  }

  trait RemoteReadableCache[V] {
    def get(key: String)(implicit ec: ExecutionContext): Future[Option[V]]
  }

  trait RemoteCacheRW[V] extends RemoteReadableCache[V] with RemoteWritableCache[V]

  trait ReporterCallback {
    def onError(key: String, t: Throwable): Unit
    def onRemoteGiveup(key: String): Unit
  }

  object NoOpReporter extends ReporterCallback {
    def onError(key: String, t: Throwable): Unit = {}
    def onRemoteGiveup(key: String): Unit = {}
  }

}


import ExpiringMultipleLevelCache._



case class ExpiringMultipleLevelCache[V](ttl: Period,
                                         localCache: LocalCache[TimestampedValue[V]],
                                         remoteRW: Option[RemoteCacheRW[TimestampedValue[V]]] = None,
                                         reporter: ExpiringMultipleLevelCache.ReporterCallback = ExpiringMultipleLevelCache.NoOpReporter,
                                         maxErrorsToRetryOnRemote: Int = 5) extends GenericCache[V] {

  private val logger = LoggerFactory.getLogger(getClass)

  private val tempUpdate = new ConcurrentLinkedHashMap.Builder[Any, Future[TimestampedValue[V]]].build()

  protected def now = DateTime.now

  private def timestamp(v: V) = TimestampedValue(now, v)

  private def remoteLockKey(key: Any) = s"$key-emlc-lock"

  private val remoteLockTTL = 10.seconds

  // This methods tries to guarantee that everyone that calls it in
  // a given moment will be left with the same value in the end
  private def remoteSetOrGet(key: String,
                             calculatedValue: TimestampedValue[V],
                             remote: RemoteCacheRW[TimestampedValue[V]],
                             currentRetry: Int = 0)(implicit ec: ExecutionContext): Future[TimestampedValue[V]] = {
    if (currentRetry > maxErrorsToRetryOnRemote) {
      reporter.onRemoteGiveup(key)
      // TODO: generate metric and log here
      // Use our calculated value as it's the best we can do
      Future.successful(calculatedValue)
    } else {
      remote.setLock(remoteLockKey(key), remoteLockTTL).asTry().flatMap {
        case Success(true) =>
          // Lock acquired, get the current value and replace it
          remote.get(key).asTry().flatMap {
            case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
              // Current value is good, just return it
              Future.successful(remoteValue)
            case Success(_) =>
              // The remote value is missing or has expired
              // We have the lock to replace this value. Our calculated value will be the canonical one!
              remote.set(key, calculatedValue).asTry().flatMap {
                case Success(_) =>
                  // Flawless victory
                  Future.successful(calculatedValue)
                case Failure(e) =>
                  // TODO: generate metric and log here
                  // Retry failure
                  remoteSetOrGet(key, calculatedValue, remote, currentRetry = currentRetry + 1)
              }
            case Failure(_) =>
              // TODO: generate metric and log here
              // Retry failure
              remoteSetOrGet(key, calculatedValue, remote, currentRetry = currentRetry + 1)
          }
        case Success(false) =>
          // Someone got the lock, let's take a look at the value
          remote.get(key).asTry().flatMap {
            case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
              // Current value is good, just return it
              Future.successful(remoteValue)
            case Success(_) =>
              // The value is missing or has expired
              // Let's start from scratch because we need to be able to set or get a good value
              // Note: do not increment retry because this isn't an error
              remoteSetOrGet(key, calculatedValue, remote, currentRetry = currentRetry)
            case Failure(e) =>
              // TODO: generate metric and log here
              // Retry
              remoteSetOrGet(key, calculatedValue, remote, currentRetry = currentRetry + 1)
          }
        case Failure(_) =>
          // TODO: generate metric and log here
          // Retry failure
          remoteSetOrGet(key, calculatedValue, remote, currentRetry = currentRetry + 1)
      }
    }
  }

  private def remoteGetWithRetryOnError(key: String,
                                        remote: RemoteCacheRW[TimestampedValue[V]],
                                        currentRetry: Int = 0)(implicit ec: ExecutionContext): Future[TimestampedValue[V]] = {
    remote.get(key).asTry().flatMap {
      case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
        Future.successful(remoteValue)
      case Success(_) =>
        Future.failed(new Exception("No good value found on remote"))
      case Failure(e) =>
        if (currentRetry >= maxErrorsToRetryOnRemote) {
          // TODO: generate metric and log here
          Future.failed(e)
        } else {
          // Retry
          remoteGetWithRetryOnError(key, remote, currentRetry = currentRetry + 1)
        }
    }
  }

  // This can be called by multiple instances simultaneously but in the end
  // only the one that wins the race will create the final value that will be set in
  // the remote caches and read by the other instances
  // Unless of course there is some error getting stuff from remote cache
  // in which case the locally generated value may be returned
  protected def canonicalValueGenerator(key: String, genValue: () => Future[V])(implicit ec: ExecutionContext) = {
    val fGeneratedValue = Try { genValue().map(timestamp) }.asFutureTry()
    val finalValue: Future[TimestampedValue[V]] = fGeneratedValue.flatMap {
      case Success(generatedValue) =>
        // Successfully generated value, try to set it in the remote writable cache
        remoteRW match {
          // No remote cache available, just return this value to be set on local cache
          case None =>
            Future.successful(generatedValue)
          case Some(remote) =>
            remoteSetOrGet(key, generatedValue, remote)
        }
      case Failure(eLocal) =>
        // We failed to generate the value ourselves, our hope is if someone else successfully did it in the meantime
        remoteRW match {
          case None =>
            // There are no remote RW caches
            // TODO: generate metric and log here
            Future.failed(eLocal)
          case Some(remote) =>
            remoteGetWithRetryOnError(key, remote).asTry().flatMap {
              case Success(v) =>
                // TODO: generate metric and log here
                Future.successful(v)
              case Failure(eRemote) =>
                // The real error is the eLocal, return it
                // TODO: generate metric and log here
                Future.failed(eLocal)
            }
        }
    }
    finalValue
  }

  // Note: this method may return a failed future, but it will never cache it
  private def tryGenerateAndSet(key: String, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[TimestampedValue[V]] = {
    val promise = Promise[TimestampedValue[V]]()
    tempUpdate.putIfAbsent(key, promise.future) match {
      case null =>
        canonicalValueGenerator(key, genValue).onComplete {
          case Success(v) =>
            localCache.set(key, Success(v))
            promise.trySuccess(v)
            tempUpdate.remove(key)
          case Failure(e) =>
            // Note: we don't save failures to cache
            promise.tryFailure(e)
            tempUpdate.remove(key)
        }
        promise.future
      case fTrying => fTrying
    }
  }

  override def apply(key: String, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[V] =
    localCache.get(key).map(_.asTry()) match {
      case Some(future) =>
        future.flatMap {
          case Success(localValue) if !localValue.hasExpired(ttl, now) =>
            // We have locally a good value, just return it
            Future.successful(localValue.value)
          case Success(expiredLocalValue) if remoteRW.nonEmpty =>
            // We have locally an expired value, but we can check a remote cache for better value
            remoteRW.get.get(key).asTry().flatMap {
              case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
                // Remote is good, set locally and return it
                localCache.set(key, Success(remoteValue))
                Future.successful(remoteValue.value)
              case Success(Some(_)) | Success(None) =>
                // No good remote, return local, async update both
                tryGenerateAndSet(key, genValue)
                Future.successful(expiredLocalValue.value)
              case Failure(e) =>
                // TODO: log, generate metrics
                tryGenerateAndSet(key, genValue)
                Future.successful(expiredLocalValue.value)
            }
          case Success(expiredLocalValue) if remoteRW.isEmpty =>
            tryGenerateAndSet(key, genValue)
            Future.successful(expiredLocalValue.value)
          case Failure(e) =>
            // This is almost impossible to happen because it's local and we don't save failed values
            // TODO: log, generate metrics
            tryGenerateAndSet(key, genValue).map(_.value)
        }
      case None if remoteRW.nonEmpty =>
        // No local, let's try remote
        remoteRW.get.get(key).asTry().flatMap {
          case Success(Some(remoteValue)) if !remoteValue.hasExpired(ttl, now) =>
            // Remote is good, set locally and return it
            localCache.set(key, Success(remoteValue))
            Future.successful(remoteValue.value)
          case Success(Some(_)) | Success(None) =>
            // No good remote, sync generate
            tryGenerateAndSet(key, genValue).map(_.value)
          case Failure(e) =>
            // TODO: log, generate metrics
            tryGenerateAndSet(key, genValue).map(_.value)
        }
      case None if remoteRW.isEmpty =>
        // No local and no remote to look, just generate it
        tryGenerateAndSet(key, genValue).map(_.value)
    }
}

case class ExpiringRedisWithAsyncUpdate()