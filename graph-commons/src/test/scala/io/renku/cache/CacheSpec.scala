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
import io.renku.interpreters.TestLogger
import io.renku.testtools.MutableClock
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._
import scala.reflect.ClassTag

class CacheSpec extends AsyncWordSpec with should.Matchers with AsyncIOSpec {
  implicit val logger: TestLogger[IO] = TestLogger[IO]()

  val clearInterval = 100.millis
  val config = CacheConfig(
    evictStrategy = EvictStrategy.Oldest,
    ignoreEmptyValues = true,
    ttl = 10.seconds,
    clearConfig = CacheConfig.ClearConfig.Periodic(10, clearInterval)
  )

  val clock: MutableClock[IO] = MutableClock.zero
  val cacheResource: Resource[IO, Cache[IO, String, Int]] = CacheBuilder
    .default[IO, String, Int]
    .withConfig(config)
    .withClock(clock)
    .resource

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

    "run the function when expired" in {
      val counter = Ref.unsafe[IO, Int](0)
      val calc: String => IO[Option[Int]] = _ => counter.updateAndGet(_ + 1).map(_.some)
      val cfg = config.copy(ttl = 0.1.second)

      CacheBuilder.default[IO, String, Int].withConfig(cfg).withClock(clock).build.flatMap { cache =>
        val calcCached = cache.withCache(calc)
        for {
          v1 <- List.fill(5)("a").traverse(calcCached)
          _ = v1 shouldBe List.fill(5)(Some(1))
          c <- counter.get
          _ = c shouldBe 1

          _ <- clock.update(_ + 1.second)

          v2 <- List.fill(5)("a").traverse(calcCached)
          _ = v2 shouldBe List.fill(5)(Some(2))
          c2 <- counter.get
          _ = c2 shouldBe 2
        } yield ()
      }
    }

    "create a noop cache when disabled via config" in {
      val counter = Ref.unsafe[IO, Int](0)
      val calc: String => IO[Option[Int]] = _ => counter.updateAndGet(_ + 1).map(_.some)
      val cfg = config.copy(ttl = 0.seconds)
      CacheBuilder.default[IO, String, Int].withConfig(cfg).withClock(clock).build.flatMap { cache =>
        val calcCached = cache.withCache(calc)
        for {
          v1 <- List.fill(5)("a").traverse(calcCached)
          _ = v1 shouldBe List.range(1, 6).map(_.some)
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

    "handlers should receive respective events" in {
      val events: Ref[IO, List[CacheEvent]] = Ref.unsafe[IO, List[CacheEvent]](Nil)
      def eventsOf[A](implicit t: ClassTag[A]) = {
        val cls = t.runtimeClass
        events.get.map(_.filter(_.getClass.isAssignableFrom(cls)))
      }

      val calc: String => IO[Option[Int]] = a => a.toIntOption.pure[IO]
      val cacheR = CacheBuilder
        .default[IO, String, Int]
        .withConfig(config)
        .withEventHandler(CacheEventHandler(ev => events.update(ev :: _)))
        .resource
      cacheR.use { cache =>
        val calcCached = cache.withCache(calc)
        for {
          _ <- List.range(0, 3).map(_.toString).traverse(calcCached)
          _ <- eventsOf[CacheEvent.CacheResponse].asserting(
                 _ shouldBe List
                   .range(1, 4)
                   .map(n => CacheEvent.CacheResponse(CacheResult.Miss, n))
                   .reverse
               )
          _ <- calcCached("0")
          _ <- eventsOf[CacheEvent.CacheResponse].asserting { list =>
                 list should have size 4
                 val CacheEvent.CacheResponse(r, n) = list.head
                 r shouldBe a[CacheResult.Hit[_]]
                 n shouldBe 3
               }

          _ <- IO.sleep(clearInterval + 50.millis)
          _ <- eventsOf[CacheEvent.CacheClear].asserting { list =>
                 list.size should be >= 1
                 val CacheEvent.CacheClear(removed, n) = list.head
                 removed shouldBe 0
                 n       shouldBe 3
               }

        } yield ()
      }
    }
  }
}
