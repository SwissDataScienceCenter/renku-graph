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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.implicits.toShow
import io.renku.eventlog.subscriptions.globalcommitsync.Generators.globalCommitSyncEvents
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GlobalCommitSyncEventSpec extends AnyWordSpec with should.Matchers {

  "show" should {

    "print out the eventId, projectPath, and lastSynced" in {
      val event = globalCommitSyncEvents.generateOne
      event.show shouldBe s"GlobalCommitSyncEvent projectId = ${event.project.id}, projectPath = ${event.project.path}, numberOfCommits = ${event.commits.count}, latestCommit = ${event.commits.latest}"
    }
  }
}
