/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.cache

import cats.syntax.all._
import io.renku.cache.CacheConfig.ClearConfig
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._

class CacheStateSpec extends AnyWordSpec with should.Matchers {

  val config = CacheConfig(
    evictStrategy = EvictStrategy.Oldest,
    ignoreEmptyValues = true,
    ttl = 10.seconds,
    clearConfig = CacheConfig.ClearConfig.Periodic(10, 2.seconds)
  )

  val key1 = new Key[String]("a", 0, 0)
  val key2 = new Key[String]("b", 10, 10)

  val secs0  = Duration.Zero
  val secs5  = 5.seconds
  val secs20 = 20.seconds

  "put" should {

    "add new values and update accessed time" in {
      val cache      = CacheState.create[String, String](config)
      val next1      = cache.put(key1, "test")
      val (next2, v) = next1.get(key1.value, secs5)
      v shouldBe CacheResult.Hit(key1.withAccessedAt(secs5), Some("test"))

      next1.data shouldBe Map(
        key1.value -> (key1, Some("test"))
      )
      next1.keys shouldBe TreeSet(key1)(config.evictStrategy.keyOrder[String])

      next2.data shouldBe Map(
        key1.value -> (key1.withAccessedAt(secs5), Some("test"))
      )
      next2.keys shouldBe TreeSet(key1.withAccessedAt(secs5))(config.evictStrategy.keyOrder[String])
    }
  }

  "return miss if expired and remove expired value" in {
    val cache      = CacheState.create[String, String](config)
    val next1      = cache.put(key1, "test")
    val (next2, v) = next1.get(key1.value, secs20)
    v     shouldBe CacheResult.Miss
    next2 shouldBe cache
  }

  "return miss for unavailable key" in {
    val cache      = CacheState.create[String, String](config)
    val next1      = cache.put(key1, "test")
    val (next2, v) = next1.get(key2.value, secs5)
    next2 shouldBe next1
    v     shouldBe CacheResult.Miss
  }

  "overwrite existing value" in {
    val cache      = CacheState.create[String, String](config).put(key1, "test1")
    val next       = cache.put(key1, "test2")
    val (next2, v) = next.get(key1.value, secs0)
    v     shouldBe CacheResult.Hit(key1, Some("test2"))
    next2 shouldBe next
  }

  "ignore empty values when configured" in {
    val cache = CacheState.create[String, String](config)
    cache.put(key1, None) shouldBe cache
  }

  "don't ignore empty values when configured" in {
    val cache = CacheState.create[String, String](config.copy(ignoreEmptyValues = false)).put(key1, None)

    val (next, v) = cache.get(key1.value, secs0)
    v              shouldBe CacheResult.Hit(key1, None)
    next.data.size shouldBe 1
  }

  "shrink to size in specified order" in {
    val cfg = config.copy(clearConfig = ClearConfig.Periodic(2, 100.millis))
    val cache = List("a", "b", "c", "d").zipWithIndex
      .map { case (v, i) => new Key(v, i, 10 - i) }
      .foldLeft(CacheState.create[String, String](cfg))((c, e) => c.put(e, e.value))

    val (next, removed) = cache.shrink(Duration.Zero)
    removed                         shouldBe 2
    next.data.size                  shouldBe cfg.clearConfig.maximumSize
    next.data.values.map(_._2).toList should contain theSameElementsAs (List(Some("c"), Some("d")))
  }

  "shrink removes all expired reducing to less than maximum size" in {
    val cfg    = config.copy(ttl = 2.seconds)
    val values = List.range('a', 'z')
    val cache = values.zipWithIndex
      .map { case (v, i) => new Key(v.toString, i, values.size - i) }
      .foldLeft(CacheState.create[String, String](cfg))((c, e) => c.put(e, e.value))

    val (next, removed) = cache.shrink(values.size.seconds)
    removed                         shouldBe (values.size - 2)
    next.data.size                  shouldBe 2
    next.data.values.map(_._2).toList should contain theSameElementsAs values.takeRight(2).map(_.toString.some)
  }

  "shrink removes all even not expired to reach maximum" in {
    val cfg    = config.copy(ttl = 2000.seconds)
    val max    = cfg.clearConfig.maximumSize
    val values = List.range('a', 'z')
    val cache = values.zipWithIndex
      .map { case (v, i) => new Key(v.toString, i, values.size - i) }
      .foldLeft(CacheState.create[String, String](cfg))((c, e) => c.put(e, e.value))

    val (next, removed) = cache.shrink(values.size.seconds)
    removed        shouldBe (values.size - max)
    next.data.size shouldBe max
  }
}
