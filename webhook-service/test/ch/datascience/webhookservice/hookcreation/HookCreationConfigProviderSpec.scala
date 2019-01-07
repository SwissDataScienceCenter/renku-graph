/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.Configuration

class HookCreationConfigProviderSpec extends WordSpec {

  "get" should {

    "return HookCreationConfig object with proper values" in {
      val gitLabUrl = validatedUrls.generateOne
      val selfUrl = validatedUrls.generateOne
      val config = Configuration.from(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "url" -> gitLabUrl.toString()
            ),
            "self" -> Map(
              "url" -> selfUrl.toString()
            )
          )
        )
      )

      new HookCreationConfigProvider[IO]( config ).get().unsafeRunSync() shouldBe HookCreationConfig(
        gitLabUrl,
        selfUrl
      )
    }

    "fail if there are no 'services.gitlab.url' and 'services.self.url' in the config" in {
      val config = Configuration.empty

      a[RuntimeException] should be thrownBy new HookCreationConfigProvider[IO]( config ).get().unsafeRunSync()
    }
  }
}
