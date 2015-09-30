package ignition.core.utils

import org.scalatest._
import CollectionUtils._

class CollectionUtilsSpec extends FlatSpec with ShouldMatchers {

  case class MyObj(property: String, value: String)
  "CollectionUtils" should "provide distinctBy" in {
    val list = List(MyObj("p1", "v1"), MyObj("p2", "v1"), MyObj("p1", "v2"), MyObj("p2", "v2"))
    list.distinctBy(_.property) shouldBe List(MyObj("p1", "v1"), MyObj("p2", "v1"))
    list.distinctBy(_.value) shouldBe List(MyObj("p1", "v1"), MyObj("p1", "v2"))
  }

  it should "provide compress" in {
    List("a", "a", "b", "c", "e", "e", "c", "d", "e").compress shouldBe List("a", "b", "c", "e", "c", "d", "e")
  }

  it should "provide compress that works on empty lists" in {
    val list = List.empty
    list.compress shouldBe list
  }

  it should "provide compress that works on lists with only one element" in {
    val list = List(MyObj("p1", "v1"))
    list.compress shouldBe list
  }

  it should "provide compressBy" in {
    val list = List(MyObj("p1", "v1"), MyObj("p2", "v1"), MyObj("p1", "v2"), MyObj("p2", "v2"))
    list.compressBy(_.property) shouldBe list
    list.compressBy(_.value) shouldBe List(MyObj("p1", "v1"), MyObj("p1", "v2"))
  }


}
