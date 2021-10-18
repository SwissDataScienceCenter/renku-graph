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

package io.renku.eventlog.events

import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages, executionDates}
import io.renku.eventlog.events.EventsEndpoint.{EventInfo, StatusProcessingTime}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.{eventIds, eventProcessingTimes, eventStatuses}
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.projects
import org.scalacheck.Gen

private object Generators {

  def eventInfos(projectPathGen: Gen[projects.Path] = projectPaths): Gen[EventInfo] = for {
    id            <- eventIds
    projectPath   <- projectPathGen
    status        <- eventStatuses
    eventDate     <- eventDates
    executionDate <- executionDates
    maybeMessage  <- eventMessages.toGeneratorOfOptions
    processingTimes <-
      statusProcessingTimeObjects(status).toGeneratorOfList().map(_.sortBy(_.status)).map(_.distinctBy(_.status))
  } yield EventInfo(id, projectPath, status, eventDate, executionDate, maybeMessage, processingTimes)

  def statusProcessingTimeObjects(lowerThan: EventStatus): Gen[StatusProcessingTime] = for {
    status         <- Gen.oneOf(EventStatus.statusesOrdered.takeWhile(Ordering[EventStatus].lteq(_, lowerThan)))
    processingTime <- eventProcessingTimes
  } yield StatusProcessingTime(status, processingTime)
}
