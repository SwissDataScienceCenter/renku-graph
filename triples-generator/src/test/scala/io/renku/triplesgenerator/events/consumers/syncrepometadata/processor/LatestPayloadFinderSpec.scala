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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.eventlog.api.EventLogClient.{EventPayload, Result, SearchCriteria}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.graph.model.EventContentGenerators.{eventDates, eventMessages, executionDates}
import io.renku.graph.model.EventsGenerators.{eventIds, eventProcessingTimes}
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.EventInfo.ProjectIds
import io.renku.graph.model.events.EventStatus.TriplesStore
import io.renku.graph.model.events.{EventId, EventInfo, EventStatus, StatusProcessingTime}
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import scodec.bits.ByteVector

import scala.util.{Failure, Success, Try}

class LatestPayloadFinderSpec extends AnyFlatSpec with should.Matchers with TryValues with MockFactory {

  it should "fetch id of the latest project event in status TRIPLES_STORE " +
    "and then fetch this event payload" in {

      val projectPath = projectPaths.generateOne
      val eventId     = eventIds.generateOne
      givenEventFinding(projectPath, returning = eventId.some.pure[Try])

      val maybePayload = Gen.option(EventPayload(ByteVector.fromValidHex("cafebabe"))).generateOne
      givenPayloadFinding(eventId, projectPath, returning = maybePayload.pure[Try])

      finder.findLatestPayload(projectPath).success.value shouldBe maybePayload
    }

  it should "return None if fetching id of the latest project event in status TRIPLES_STORE returns no results" in {

    val projectPath = projectPaths.generateOne
    givenEventFinding(projectPath, returning = Option.empty.pure[Try])

    finder.findLatestPayload(projectPath).success.value shouldBe None
  }

  it should "fail if finding eventId fails" in {

    val projectPath = projectPaths.generateOne
    val exception   = exceptions.generateOne
    givenEventFindingResponding(projectPath, exception.raiseError[Try, Nothing])

    finder.findLatestPayload(projectPath).failure.exception shouldBe exception
  }

  it should "fail if finding eventId returns a failure" in {

    val projectPath = projectPaths.generateOne
    val exception   = nonEmptyStrings().generateOne
    givenEventFindingResponding(projectPath, Result.failure(exception).pure[Try])

    finder.findLatestPayload(projectPath).failure.exception.getMessage shouldBe exception
  }

  it should "fail if finding eventId returns unavailable" in {

    val projectPath = projectPaths.generateOne
    givenEventFindingResponding(projectPath, Result.unavailable.pure[Try])

    finder.findLatestPayload(projectPath).failure.exception.getMessage shouldBe Result.Unavailable.getMessage
  }

  it should "fail if finding payload fails" in {

    val projectPath = projectPaths.generateOne
    val eventId     = eventIds.generateOne
    givenEventFinding(projectPath, returning = eventId.some.pure[Try])

    val exception = exceptions.generateOne
    givenPayloadFinding(eventId, projectPath, returning = exception.raiseError[Try, Nothing])

    finder.findLatestPayload(projectPath).failure.exception shouldBe exception
  }

  it should "fail if finding payload returns a failure" in {

    val projectPath = projectPaths.generateOne
    val eventId     = eventIds.generateOne
    givenEventFinding(projectPath, returning = eventId.some.pure[Try])

    val exception = nonEmptyStrings().generateOne
    givenPayloadFindingResponding(eventId, projectPath, Result.failure(exception).pure[Try])

    finder.findLatestPayload(projectPath).failure.exception.getMessage shouldBe exception
  }

  it should "fail if finding payload returns unavailable" in {

    val projectPath = projectPaths.generateOne
    val eventId     = eventIds.generateOne
    givenEventFinding(projectPath, returning = eventId.some.pure[Try])

    givenPayloadFindingResponding(eventId, projectPath, Result.unavailable.pure[Try])

    finder.findLatestPayload(projectPath).failure.exception.getMessage shouldBe Result.Unavailable.getMessage
  }

  private lazy val elClient = mock[EventLogClient[Try]]
  private lazy val finder   = new LatestPayloadFinderImpl[Try](elClient)

  private def givenEventFinding(path: projects.Path, returning: Try[Option[EventId]]) =
    givenEventFindingResponding(path, toEventsFindingResult(path, returning))

  private def givenEventFindingResponding(path: projects.Path, response: Try[Result[List[EventInfo]]]) =
    (elClient.getEvents _)
      .expects(
        SearchCriteria
          .forProject(path)
          .withStatus(TriplesStore)
          .withPerPage(PerPage(1))
          .sortBy(SearchCriteria.Sort.EventDateDesc)
      )
      .returning(response)

  private def toEventsFindingResult(path: projects.Path, returning: Try[Option[EventId]]) =
    returning match {
      case Success(Some(eventId)) =>
        val status = EventStatus.TriplesStore
        Result
          .Success(
            List(
              EventInfo(
                eventId,
                ProjectIds(projectIds.generateOne, path),
                status,
                eventDates.generateOne,
                executionDates.generateOne,
                eventMessages.generateOption,
                eventProcessingTimes.generateFixedSizeList(ofSize = 1).map(StatusProcessingTime(status, _))
              )
            )
          )
          .pure[Try]
      case Success(None)      => Result.Success(List.empty[EventInfo]).pure[Try]
      case Failure(exception) => exception.raiseError[Try, Result[List[EventInfo]]]
    }

  private def givenPayloadFinding(eventId: EventId, path: projects.Path, returning: Try[Option[EventPayload]]) = {

    val result = returning match {
      case Success(maybePayload) => Result.Success(maybePayload).pure[Try]
      case Failure(exception)    => exception.raiseError[Try, Result[Option[EventPayload]]]
    }

    givenPayloadFindingResponding(eventId, path, result)
  }

  private def givenPayloadFindingResponding(eventId:  EventId,
                                            path:     projects.Path,
                                            response: Try[Result[Option[EventPayload]]]
  ) = (elClient.getEventPayload _)
    .expects(eventId, path)
    .returning(response)
}
