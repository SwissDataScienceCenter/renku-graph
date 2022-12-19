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

package io.renku.graph.model

import io.renku.generators.Generators.{nonEmptyStrings, timestamps, timestampsNotInTheFuture}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.{eventIds, eventProcessingTimes, eventStatuses}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.EventInfo.ProjectIds
import io.renku.graph.model.events._
import org.scalacheck.Gen

object EventContentGenerators {

  implicit val eventDates:     Gen[EventDate]     = timestampsNotInTheFuture map EventDate.apply
  implicit val createdDates:   Gen[CreatedDate]   = timestampsNotInTheFuture map CreatedDate.apply
  implicit val executionDates: Gen[ExecutionDate] = timestamps map ExecutionDate.apply

  implicit val eventMessages: Gen[EventMessage] = nonEmptyStrings() map EventMessage.apply

  def eventInfos(
      projectPathGen: Gen[projects.Path] = projectPaths,
      projectIdGen:   Gen[projects.GitLabId] = projectIds
  ): Gen[EventInfo] = for {
    id            <- eventIds
    projectPath   <- projectPathGen
    projectId     <- projectIdGen
    status        <- eventStatuses
    eventDate     <- eventDates
    executionDate <- executionDates
    maybeMessage  <- eventMessages.toGeneratorOfOptions
    processingTimes <-
      statusProcessingTimeObjects(status).toGeneratorOfList().map(_.sortBy(_.status)).map(_.distinctBy(_.status))
  } yield EventInfo(id,
                    ProjectIds(projectId, projectPath),
                    status,
                    eventDate,
                    executionDate,
                    maybeMessage,
                    processingTimes
  )

  def statusProcessingTimeObjects(lowerThan: EventStatus): Gen[StatusProcessingTime] = for {
    status         <- Gen.oneOf(EventStatus.statusesOrdered.takeWhile(Ordering[EventStatus].lteq(_, lowerThan)))
    processingTime <- eventProcessingTimes
  } yield StatusProcessingTime(status, processingTime)
}
