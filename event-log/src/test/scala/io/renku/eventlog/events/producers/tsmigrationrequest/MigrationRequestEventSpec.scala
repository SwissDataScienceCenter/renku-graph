/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.tsmigrationrequest

import cats.syntax.all._
import io.circe.literal._
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MigrationRequestEventSpec extends AnyWordSpec with should.Matchers {

  "encodeEvent" should {

    "serialize MemberSyncEvent to Json" in {
      val event = events.generateOne

      MigrationRequestEvent.encodeEvent(event) shouldBe json"""{
        "categoryName": "TS_MIGRATION_REQUEST",
        "subscriber": {
          "version": ${event.subscriberVersion.value}
        }
      }"""
    }
  }

  "show" should {

    "return String representation of the event containing url and version" in {
      val event = events.generateOne

      event.show shouldBe show"""subscriberVersion = ${event.subscriberVersion}"""
    }
  }

  private lazy val events: Gen[MigrationRequestEvent] = for {
    url     <- subscriberUrls
    version <- serviceVersions
  } yield MigrationRequestEvent(url, version)
}
