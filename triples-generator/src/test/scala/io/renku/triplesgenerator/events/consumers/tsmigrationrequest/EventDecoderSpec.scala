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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.syntax.all._
import io.circe.literal._
import io.circe.DecodingFailure
import io.renku.events.EventRequestContent
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues

class EventDecoderSpec extends AnyWordSpec with should.Matchers with EitherValues {

  private val tgServiceVersion = serviceVersions.generateOne

  "decode" should {

    "succeed if event version and tg version matches" in {

      val eventServiceVersion = tgServiceVersion
      lazy val event = json"""{
        "categoryName": "TS_MIGRATION_REQUEST",
        "subscriber": {
          "version": $eventServiceVersion
        }
      }"""

      EventDecoder.decode(tgServiceVersion)(EventRequestContent.NoPayload(event)).value shouldBe ()
    }

    "fail if event version and tg version do not match" in {

      val eventServiceVersion = serviceVersions.generateOne
      lazy val event =
        json"""{
        "categoryName": "TS_MIGRATION_REQUEST",
        "subscriber": {
          "version": $eventServiceVersion
        }
      }"""

      EventDecoder
        .decode(tgServiceVersion)(EventRequestContent.NoPayload(event))
        .left
        .value
        .getMessage shouldBe show"Service in version '$tgServiceVersion' but event for '$eventServiceVersion'"
    }

    "fail if decoding fails" in {
      EventDecoder
        .decode(tgServiceVersion)(EventRequestContent.NoPayload(json"{}"))
        .left
        .value shouldBe a[DecodingFailure]
    }
  }
}
