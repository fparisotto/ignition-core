// Note:
// For ignition.core we added two methods to satisfy ExpiringMultipleLevelCache.LocalCache[V]

/*
 * Copyright © 2011-2013 the spray project <http://spray.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spray.caching

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import spray.util.Timestamp

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

final class ExpiringLruLocalCache[V](maxCapacity: Long,
                                     initialCapacity: Int = 16,
                                     timeToLive: Duration = Duration.Inf,
                                     timeToIdle: Duration = Duration.Inf) extends Cache[V] with ignition.core.cache.ExpiringMultiLevelCache.LocalCache[V] {
  require(!timeToLive.isFinite || !timeToIdle.isFinite || timeToLive > timeToIdle,
    s"timeToLive($timeToLive) must be greater than timeToIdle($timeToIdle)")

  private[caching] val store = new ConcurrentLinkedHashMap.Builder[Any, Entry[V]]
    .initialCapacity(initialCapacity)
    .maximumWeightedCapacity(maxCapacity)
    .build()

  @tailrec
  def get(key: Any): Option[Future[V]] = store.get(key) match {
    case null ⇒ None
    case entry if (isAlive(entry)) ⇒
      entry.refresh()
      Some(entry.future)
    case entry ⇒
      // remove entry, but only if it hasn't been removed and reinserted in the meantime
      if (store.remove(key, entry)) None // successfully removed
      else get(key) // nope, try again
  }

  def apply(key: Any, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V] = {
    def insert() = {
      val newEntry = new Entry(Promise[V]())
      val valueFuture =
        store.put(key, newEntry) match {
          case null ⇒ genValue()
          case entry ⇒
            if (isAlive(entry)) {
              // we date back the new entry we just inserted
              // in the meantime someone might have already seen the too fresh timestamp we just put in,
              // but since the original entry is also still alive this doesn't matter
              newEntry.created = entry.created
              entry.future
            } else genValue()
        }
      valueFuture.onComplete { value ⇒
        newEntry.promise.tryComplete(value)
        // in case of exceptions we remove the cache entry (i.e. try again later)
        if (value.isFailure) store.remove(key, newEntry)
      }
      newEntry.promise.future
    }
    store.get(key) match {
      case null ⇒ insert()
      case entry if (isAlive(entry)) ⇒
        entry.refresh()
        entry.future
      case entry ⇒ insert()
    }
  }

  def remove(key: Any) = store.remove(key) match {
    case null                      ⇒ None
    case entry if (isAlive(entry)) ⇒ Some(entry.future)
    case entry                     ⇒ None
  }

  def clear(): Unit = { store.clear() }

  def keys: Set[Any] = store.keySet().asScala.toSet

  def ascendingKeys(limit: Option[Int] = None) =
    limit.map { lim ⇒ store.ascendingKeySetWithLimit(lim) }
      .getOrElse(store.ascendingKeySet())
      .iterator().asScala

  def size = store.size

  private def isAlive(entry: Entry[V]) =
    (entry.created + timeToLive).isFuture &&
      (entry.lastAccessed + timeToIdle).isFuture

  // Method required by ExpiringMultipleLevelCache.LocalCache
  override def set(key: Any, value: V): Unit = {
    val newEntry = new Entry(Promise[V]())
    newEntry.promise.trySuccess(value)
    store.put(key, newEntry) match {
      case null =>
        // Nothing to do
      case oldEntry =>
        // If the old promise is pending, complete it with our future
        oldEntry.promise.trySuccess(value)
    }
  }
}

private[caching] class ExpiringLruLocalCacheEntry[T](val promise: Promise[T]) {
  @volatile var created = Timestamp.now
  @volatile var lastAccessed = Timestamp.now
  def future = promise.future
  def refresh(): Unit = {
    // we dont care whether we overwrite a potentially newer value
    lastAccessed = Timestamp.now
  }
  override def toString = future.value match {
    case Some(Success(value))     ⇒ value.toString
    case Some(Failure(exception)) ⇒ exception.toString
    case None                     ⇒ "pending"
  }
}
