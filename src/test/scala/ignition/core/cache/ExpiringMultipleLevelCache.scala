package ignition.core.cache

import akka.actor.ActorSystem
import ignition.core.cache.ExpiringMultiLevelCache.TimestampedValue
import org.scalatest.{FlatSpec, Matchers}
import spray.caching.ExpiringLruLocalCache

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ExpiringMultipleLevelCacheSpec extends FlatSpec with Matchers {
  case class Data(s: String)
  implicit val scheduler = ActorSystem().scheduler

  "ExpiringMultipleLevelCache" should "calculate a value on cache miss and return it" in {
    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](1.minute, Option(local))
    Await.result(cache("key", () => Future.successful(Data("success"))), 1.minute) shouldBe Data("success")
  }

  it should "calculate a value on cache miss and return a failed future of the calculation" in {
    val local = new ExpiringLruLocalCache[TimestampedValue[Data]](100)
    val cache = ExpiringMultiLevelCache[Data](1.minute, Option(local))

    class MyException(s: String) extends Exception(s)

    intercept[MyException ] {
      Await.result(cache("key", () => Future.failed(new MyException("some failure"))), 1.minute)
    }
  }
}
