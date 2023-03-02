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

package io.renku.eventlog.events.consumers.statuschange

import io.circe.Json
import io.circe.syntax._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.ZippedEventPayload
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatusChangeEventSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  // TODO does this make sense to keep?
  "StatusChangeEvent" should {

    "be serializable and deserializable to and from Json" in {
      forAll(StatusChangeGenerators.statusChangeEvents) { event =>
        val ev = event match {
          case ttg: ToTriplesGenerated => ttg.copy(payload = ZippedEventPayload.empty)
          case _ => event
        }

        ev.asJson.as[StatusChangeEvent] shouldBe Right(ev)
      }
    }
  }

  "decode" should {

    "use discriminators to distinguish specific types" in {
      val triplesGenerated: ToTriplesGenerated = StatusChangeGenerators.toTriplesGeneratedEvents.generateOne
      val tripleGeneratedJson = triplesGenerated.asJson
        .deepMerge(Json.obj("subCategory" -> "RollbackToTriplesGenerated".asJson))

      // the json structure is almost identical for RollbackToTriplesGenerated and ToTriplesGenerated
      // changing the subCategory must pickup the other decoder
      val decoded = tripleGeneratedJson.as[StatusChangeEvent]
      decoded shouldBe Right(RollbackToTriplesGenerated(triplesGenerated.id, triplesGenerated.project))
    }

    "decode to empty payload for ToTriplesGenerated" in {
      val event = StatusChangeGenerators.toTriplesGeneratedEvents.generateOne
      event.payload.value should not be empty
      val decoded = event.asJson.as[ToTriplesGenerated].fold(throw _, identity)
      decoded shouldBe event.copy(payload = ZippedEventPayload.empty)
    }
  }
  "encode" should {
    "encode a discriminator value" in {
      forAll(StatusChangeGenerators.statusChangeEvents) { event =>
        val json        = event.asJson
        val subCategory = json.asObject.flatMap(_.apply("subCategory"))
        subCategory shouldBe Some(event.subCategoryName.asJson)
      }
    }

    "encode a discriminator value when using specific codec" in {
      val event: RollbackToAwaitingDeletion = StatusChangeGenerators.rollbackToAwaitingDeletionEvents.generateOne
      val subCategory = event.asJson(RollbackToAwaitingDeletion.jsonEncoder).asObject.flatMap(_.apply("subCategory"))
      subCategory shouldBe Some(event.subCategoryName.asJson)
    }

    "remove null values" in {
      val event = StatusChangeGenerators.toFailureEvents.generateOne
        .copy(executionDelay = None)

      val json = event.asJson.asObject.getOrElse(sys.error("Expected a json object"))
      json("executionDelay") shouldBe None
    }

    "not encode payload for triples-generated" in {
      val event = StatusChangeGenerators.toTriplesGeneratedEvents.suchThat(_.payload.value.nonEmpty).generateOne
      val json  = event.asJson.asObject.getOrElse(sys.error("Expected a json object"))
      json("payload") shouldBe None
    }
  }

}
