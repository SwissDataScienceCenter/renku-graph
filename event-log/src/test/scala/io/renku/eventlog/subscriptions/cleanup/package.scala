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

package io.renku.eventlog.subscriptions

import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.EventsGenerators.eventIds
import org.scalacheck.Gen
import io.renku.events.consumers.Project
import io.renku.graph.model.events.CompoundEventId

package object cleanup {

  private[cleanup] lazy val cleanupEvents: Gen[CleanUpEvent] = for {
    eventId     <- eventIds
    projectId   <- projectIds
    projectPath <- projectPaths
  } yield CleanUpEvent(CompoundEventId(eventId, projectId), Project(projectId, projectPath))
}
