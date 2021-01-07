/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.unprocessed

import ch.datascience.generators.Generators.Implicits._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UnprocessedEventEncoderSpec extends AnyWordSpec with should.Matchers {

  private implicit val encoder: Encoder[UnprocessedEvent] = UnprocessedEventEncoder

  "encoder" should {

    "serialize UnprocessedEvent to Json" in {
      val event = unprocessedEvents.generateOne

      event.asJson shouldBe json"""{
        "id":      ${event.id.id.value},
        "project": {
          "id":    ${event.id.projectId.value}
        },
        "body":    ${event.body.value}
      }"""
    }
  }
}
