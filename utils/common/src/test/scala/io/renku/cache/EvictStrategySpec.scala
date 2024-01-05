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

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should

import scala.concurrent.duration._

class EvictStrategySpec extends AnyWordSpec with should.Matchers {

  "Oldest" should {
    "expired based on createdAt" in {
      val s   = EvictStrategy.Oldest
      val ttl = 20.seconds
      val key = new Key(0, 10, 0)
      s.isExpired(key, ttl, 2.seconds)  shouldBe false
      s.isExpired(key, ttl, 22.seconds) shouldBe false
      s.isExpired(key, ttl, 32.seconds) shouldBe true
    }

    "expired based on accessedAt" in {
      val s   = EvictStrategy.LeastRecentlyUsed
      val ttl = 20.seconds
      val key = new Key(0, 0, 10)
      s.isExpired(key, ttl, 2.seconds)  shouldBe false
      s.isExpired(key, ttl, 22.seconds) shouldBe false
      s.isExpired(key, ttl, 32.seconds) shouldBe true
    }
  }
}
