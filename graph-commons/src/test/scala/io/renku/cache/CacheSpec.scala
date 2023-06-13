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

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class CacheSpec extends AsyncWordSpec with should.Matchers with AsyncIOSpec {

  val clearInterval = 100.millis
  val config = CacheConfig(
    evictStrategy = EvictStrategy.Oldest,
    ignoreEmptyValues = true,
    ttl = 10.seconds,
    clearConfig = CacheConfig.ClearConfig.Periodic(10, clearInterval)
  )

  val cacheResource: Resource[IO, Cache[IO, String, Int]] = Cache.memoryAsync[IO, String, Int](config)

  val key1 = new Key[String]("a", 0, 0)

  "Cache" should {
    "not run the function on a hit" in {
      val counter = Ref.unsafe[IO, Int](0)
      val calc: String => IO[Option[Int]] = _ => counter.updateAndGet(_ + 1).map(_.some)

      cacheResource.use { cache =>
        val calcCached = cache.withCache(calc)
        for {
          v1 <- List.fill(5)("a").traverse(calcCached)
          _ = v1 shouldBe List.fill(5)(Some(1))
          c <- counter.get
          _ = c shouldBe 1
        } yield ()
      }
    }

    "remove values periodically" in {
      val counter = Ref.unsafe[IO, Int](0)
      val calc: String => IO[Option[Int]] = _ => counter.updateAndGet(_ + 1).map(_.some)
      cacheResource.use { cache =>
        val calcCached = cache.withCache(calc)
        for {
          _ <- List.range(0, 100).map(_.toString).traverse(calcCached)
          _ <- counter.set(0)
          _ <- IO.sleep(clearInterval + 50.millis)
          _ <- List.range(0, 100).map(_.toString).traverse(calcCached)
          c <- counter.get
          _ = c shouldBe (100 - config.clearConfig.maximumSize)
        } yield ()
      }
    }
  }
}
