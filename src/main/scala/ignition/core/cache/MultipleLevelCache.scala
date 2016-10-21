package ignition.core.cache

import ignition.core.utils.FutureUtils._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait SimpleCache[V] {
  def apply(key: Any, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V]
}

trait LocalCache[V] extends SimpleCache[V] {
  def get(key: Any): Option[Future[V]]
  def set(key: Any, value: Try[V]): Boolean
}

trait RemoteWritableCache[V] {
  def set(key: Any, value: Try[V])(implicit ec: ExecutionContext): Future[Boolean]
}

trait RemoteReadableCache[V] {
  def get(key: Any)(implicit ec: ExecutionContext): Future[Option[V]]
}

trait RemoteCacheRW[V] extends SimpleCache[V] with RemoteReadableCache[V] with RemoteWritableCache[V]


case class LocalAsRemote[V](local: LocalCache[V]) extends RemoteCacheRW[V] {
  override def get(key: Any)(implicit ec: ExecutionContext): Future[Option[V]] =
    local.get(key).map(_.map(Option.apply)).getOrElse(Future.successful(None))

  override def set(key: Any, value: Try[V])(implicit ec: ExecutionContext): Future[Boolean] =
    Future.successful(local.set(key, value))

  override def apply(key: Any, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[V] =
    apply(key, genValue)
}

case class MultipleLevelCache[V](localCache: LocalCache[V],
                                 remoteRW: List[RemoteCacheRW[V]],
                                 remoteReadOnly: List[RemoteReadableCache[V]]) extends SimpleCache[V] {
  val allReadableCaches: Array[RemoteReadableCache[V]] =
    (LocalAsRemote(localCache) +: (remoteRW ++ remoteReadOnly)).toArray

  // This can be called by multiple instances simultaneously but in the end
  // only the one that wins the race will create the final value that will be set in
  // the remote caches and read by the other instances
  // Unless of course there is some error getting stuff from remote cache
  // in which case the local value may be returned
  def canonicalValueGenerator(key: Any, genValue: () => Future[V])() = {
    val fLocalValue = genValue()
    val finalValue: Future[V] = fLocalValue.asTry().flatMap {
      case tLocalValue @ Success(localValue) =>
        // Successfully generated value, try to set it in the first remote Writable cache
        remoteRW match {
          // No remote cache available, just return this value to be set on local cache
          case Nil =>
            Future.successful(localValue)
          // We have at least one remote RW cache
          case first :: others =>
            first.set(key, tLocalValue).asTry().flatMap {
              case Success(true) =>
                // Successfully inserted on first remote store, propagate value to other remote rw caches
                // We do it in a fire and forget approach, we only guarantee the data is in the first cache
                others.foreach(_.set(key, tLocalValue))
                // Return this value to be set on the local cache
                Future.successful(localValue)
              case Success(false) =>
                // There is already a value there, we lost the race, ours won't be the canonical one, try to get it
                first.get(key).asTry().flatMap {
                  case Success(Some(remoteValue)) =>
                    // Just return it
                    Future.successful(remoteValue)
                  case Success(None) =>
                    // WTF? the set operation said it was there but now the value disappeared?!
                    // So return our value which is good and hope for the best
                    // TODO: generate metric and log here
                    Future.successful(localValue)
                  case Failure(_) =>
                    // Oh noes, we failed to get the canonical value
                    // We are supposing any retries have already been done by the cache implementation
                    // So return our value which is good and hope for the best
                    // TODO: generate metric and log here
                    Future.successful(localValue)
                }
              case Failure(_) =>
                // Oh noes, we failed to set the canonical value
                // We are supposing any retries have already been done by the cache implementation
                // So return our value which is good and hope for the best
                // TODO: generate metric and log here
                Future.successful(localValue)
            }
        }
      case Failure(eLocal) =>
        // We failed to generate the value ourselves, our only hope is if someone else successfully did it in the meantime
        remoteRW match {
          case Nil =>
            // There are no remote RW caches
            // FIXME: perhaps try the read only caches (but we can just be wasting time doing that)
            // TODO: generate metric and log here
            Future.failed(eLocal)
          case first :: others =>
            first.get(key).asTry().flatMap {
              case Success(Some(remoteValue)) =>
                // Hooray, someone calculated and set the value, return it
                Future.successful(remoteValue)
              case Success(None) =>
                // Sadly, there is no value on remote, we failed!
                // FIXME: perhaps try other caches (but we can just be wasting time doing that)
                // TODO: generate metric and log here
                Future.failed(eLocal)
              case Failure(eRemote) =>
                // Oh noes, this failed
                // We are supposing any retries have already been done by the cache implementation
                // And to make things worse, we don't have a good value
                // So return a failure
                // FIXME: perhaps try other caches (but we can just be wasting time doing that)
                // TODO: generate metric and log here
                Future.failed(eLocal)
            }
        }
    }
    finalValue
  }

  def indexedApply(index: Int, key: Any, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[V] = {
    if (index >= allReadableCaches.size) { // nothing found on our caches, calculate value
      // We could generate the value then set on local cache, but calling apply guarantees
      // canonicalValueGenerator will be called only once in this instance (supposing LocalCache works like Spray Cache)
      localCache(key, canonicalValueGenerator(key, genValue))
    } else {
      allReadableCaches(index).get(key).asTry().flatMap {
        case Success(None) =>
          // Try next cache
          indexedApply(index + 1, key, genValue)
        case Success(Some(value)) =>
          Future.successful(value)
        case Failure(e) =>
          // Oh noes, this failed
          // We are supposing any retries have already been done by the cache implementation
          // So try the next one, we don't have many options
          // TODO: generate metric and log here
          indexedApply(index + 1, key, genValue)
      }
    }
  }

  override def apply(key: Any, genValue: () => Future[V])(implicit ec: ExecutionContext): Future[V] =
    indexedApply(0, key, genValue)
}

case class ExpiringRedisWithAsyncUpdate()