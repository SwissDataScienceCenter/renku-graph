/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.Monad
import cats.effect.IO
import io.renku.testtools.{IOSpec, MutableClock}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._

class KeySpec extends AnyWordSpec with should.Matchers with IOSpec with ScalaCheckPropertyChecks {

  "Key" should {
    "create based on current time" in {
      val clock = MutableClock.apply(151.seconds)
      val key   = Key[IO, Int](15)(clock, Monad[IO]).unsafeRunSync()
      key.value      shouldBe 15
      key.createdAt    should be >= 151
      key.accessedAt   should be >= 151
      key.accessedAt shouldBe key.createdAt
    }

    "retain order based on the respective property" in {
      forAll { values: List[String] =>
        val keys = values.distinct.zipWithIndex.map { case (v, ts) =>
          new Key(v, ts, values.size - ts)
        }

        keys.sorted(Key.Order.oldest[String])            shouldBe keys.sortBy(_.createdAt)
        keys.sorted(Key.Order.leastRecentlyUsed[String]) shouldBe keys.sortBy(_.accessedAt)
      }
    }

    "distinguish different values with same properties" in {
      forAll { values: List[String] =>
        val keys = values.distinct.map(v => new Key(v, 1, 1))

        // order doesn't matter, but all values must be present
        TreeSet.from(keys)(Key.Order.oldest[String]).size            shouldBe keys.size
        TreeSet.from(keys)(Key.Order.leastRecentlyUsed[String]).size shouldBe keys.size
      }
    }
  }
}
