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

import cats.implicits.toShow
import io.renku.eventlog.subscriptions.commitsync.Generators.{fullCommitSyncEvents, minimalCommitSyncEvents}
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommitSyncEventSpec extends AnyWordSpec with should.Matchers {

  "FullCommitSyncEvent.show" should {

    "print out the eventId, projectPath, and lastSynced" in {
      val event = fullCommitSyncEvents.generateOne
      event.show shouldBe s"CommitSyncEvent ${event.id}, projectPath = ${event.projectPath}, lastSynced = ${event.lastSyncedDate}"
    }
  }

  "MinimalCommitSyncEvent.show" should {

    "print out the eventId, projectPath, and lastSynced" in {
      val event = minimalCommitSyncEvents.generateOne
      event.show shouldBe s"CommitSyncEvent projectId = ${event.project.id}, projectPath = ${event.project.path}"
    }
  }
}
