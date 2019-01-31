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

package ch.datascience.graph.events

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.tinytypes.Sensitive
import io.circe.Json
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class HookAccessTokenSpec extends WordSpec {

  "HookAccessToken" should {

    "be Sensitive" in {
      hookAccessTokens.generateOne shouldBe a[Sensitive]
    }

    "be deserializable from String Json" in {
      val token = hookAccessTokens.generateOne

      val result = HookAccessToken.hookAccessTokenDecoder.decodeJson(Json.fromString(token.value))

      result.fold(
        error => fail(s"HookAccessToken deserialization does not work: $error"),
        deserialised => deserialised shouldBe token
      )
    }
  }
}
