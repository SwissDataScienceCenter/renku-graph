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

package io.renku.eventlog.subscriptions.commitsync

import ch.datascience.generators.Generators.Implicits._
import io.circe.literal._
import io.renku.eventlog.subscriptions.commitsync.Generators.commitSyncEvents
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommitSyncEventEncoderSpec extends AnyWordSpec with should.Matchers {

  "encodeEvent" should {

    "serialize AwaitingGenerationEvent to Json" in {
      val event = commitSyncEvents.generateOne

      CommitSyncEventEncoder.encodeEvent(event) shouldBe json"""{
        "categoryName": "COMMIT_SYNC",
        "id":           ${event.id.id.value},
        "project": {
          "id":         ${event.id.projectId.value},
          "path":       ${event.id.projectId.value}
        },
        "lastSynced":   ${event.lastSyncedDate.value}
      }"""
    }
  }

  "encodePayload" should {
    "return None" in {
      CommitSyncEventEncoder.encodePayload(commitSyncEvents.generateOne) shouldBe None
    }
  }
}
