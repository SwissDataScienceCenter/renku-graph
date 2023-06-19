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

import com.typesafe.config.ConfigFactory
import io.renku.cache.CacheConfig.ClearConfig.Periodic
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class CacheConfigLoaderSpec extends AnyWordSpec with should.Matchers {

  "CacheConfigLoader" should {
    "load from given config" in {
      val cfg = ConfigFactory.parseString("""evict-strategy = oldest
                                            |ignore-empty-values = true
                                            |ttl = 5s
                                            |clear-config = {
                                            |  type = periodic
                                            |  maximum-size = 1000
                                            |  interval = 10min
                                            |}
                                            |""".stripMargin)

      val cc = CacheConfigLoader.unsafeRead(cfg)
      cc shouldBe CacheConfig(EvictStrategy.Oldest, true, 5.seconds, Periodic(1000, 10.minutes))
    }
  }
}
