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

package io.renku.eventlog.events.producers.commitsync

import Generators._
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommitSyncEventEncoderSpec extends AnyWordSpec with should.Matchers {

  "encodeEvent" should {

    "serialize FullCommitSyncEvent to Json" in {
      val event = fullCommitSyncEvents.generateOne

      CommitSyncEventEncoder.encodeEvent(event) shouldBe json"""{
        "categoryName": "COMMIT_SYNC",
        "id":           ${event.id.id.value},
        "project": {
          "id":         ${event.id.projectId.value},
          "path":       ${event.projectPath.value}
        },
        "lastSynced":   ${event.lastSyncedDate.value}
      }"""
    }

    "serialize MinimalCommitSyncEvent to Json" in {
      val event = minimalCommitSyncEvents.generateOne

      CommitSyncEventEncoder.encodeEvent(event) shouldBe json"""{
        "categoryName": "COMMIT_SYNC",
        "project": {
          "id":         ${event.project.id.value},
          "path":       ${event.project.path.value}
        }
      }"""
    }
  }
}