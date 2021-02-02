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

package ch.datascience.triplesgenerator.events
package categories.awaitinggeneration.subscriptions

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.positiveInts
import ch.datascience.graph.model.EventsGenerators.categoryNames
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.GenerationProcessesNumber
import io.circe.Encoder._
import io.circe.Json
import io.circe.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PayloadSpec extends AnyWordSpec with should.Matchers {

  "encoder" should {

    "encode the given CategoryAndUrlPayload to Json" in {
      val payload = payloads.generateOne
      payload.asJson(Payload.encoder) shouldBe Json.obj(
        "categoryName"  -> encodeString(payload.categoryName.value),
        "subscriberUrl" -> encodeString(payload.subscriberUrl.value),
        "capacity"      -> encodeInt(payload.capacity.value)
      )
    }
  }

  private lazy val payloads: Gen[Payload] = for {
    categoryName  <- categoryNames
    subscriberUrl <- subscriptions.subscriberUrls
    capacity      <- positiveInts().map(v => GenerationProcessesNumber(v.value))
  } yield Payload(categoryName, subscriberUrl, capacity)
}
