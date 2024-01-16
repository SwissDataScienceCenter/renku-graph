/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.api.events

import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import org.scalacheck.Gen

object Generators extends StatusChangeGenerators {

  val commitSyncRequests: Gen[CommitSyncRequest] =
    consumerProjects.map(CommitSyncRequest.apply)

  val globalCommitSyncRequests: Gen[GlobalCommitSyncRequest] =
    consumerProjects.map(GlobalCommitSyncRequest.apply)

  val cleanUpRequests: Gen[CleanUpRequest] =
    Gen.oneOf(
      consumerProjects.map(CleanUpRequest.apply),
      projectSlugs.map(CleanUpRequest.apply)
    )
}
