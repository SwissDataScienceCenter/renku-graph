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

package io.renku.eventlog.api

import EventLogClient.{Result, SearchCriteria}
import Http4sEventLogClientSpec.JsonEncoders._
import cats.effect._
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.jsons
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import io.renku.graph.model._
import io.renku.graph.model.events.{EventDate, EventInfo, EventStatus, StatusProcessingTime}
import io.renku.http.client.UrlEncoder
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.{Status, Uri}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger
import scodec.bits.ByteVector

import java.time.temporal.ChronoUnit

class Http4sEventLogClientSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with EitherValues {

  implicit val logger: Logger[IO] = TestLogger()

  val client = new Http4sEventLogClient[IO](Uri.unsafeFromString(externalServiceBaseUrl))

  "getEvents" should {

    "apply search criteria in the query - case with status query param" in {
      val events    = EventContentGenerators.eventInfos().toGeneratorOfNonEmptyList().generateOne.toList
      val sinceDate = EventContentGenerators.eventDates.generateOne.value.truncatedTo(ChronoUnit.SECONDS)

      stubFor {
        get(
          s"/events?status=${EventStatus.TriplesStore.value}&since=${param(sinceDate.toString)}&page=1&per_page=35&sort=eventDate%3AASC"
        ).willReturn(ok(events))
      }

      val criteria = SearchCriteria
        .forStatus(EventStatus.TriplesStore)
        .withSince(EventDate(sinceDate))
        .withPerPage(35)
        .sortBy(SearchCriteria.Sort.EventDateAsc)

      val result = client.getEvents(criteria).unsafeRunSync()
      result.toEither.fold(throw _, identity) shouldBe events
    }

    "apply search criteria in the query - case with project slug query param" in {
      val events      = EventContentGenerators.eventInfos().toGeneratorOfNonEmptyList().generateOne.toList
      val projectSlug = projectSlugs.generateOne
      val sinceDate   = EventContentGenerators.eventDates.generateOne.value.truncatedTo(ChronoUnit.SECONDS)

      stubFor {
        get(
          s"/events?since=${param(sinceDate.toString)}&project-slug=${projectSlug.show}&page=1&per_page=35&sort=eventDate%3AASC"
        ).willReturn(ok(events))
      }

      val criteria = SearchCriteria
        .forProject(projectSlug)
        .withSince(EventDate(sinceDate))
        .withPerPage(35)
        .sortBy(SearchCriteria.Sort.EventDateAsc)

      val result = client.getEvents(criteria).unsafeRunSync()
      result.toEither.fold(throw _, identity) shouldBe events
    }

    "apply search criteria in the query - case with project id query param" in {
      val events    = EventContentGenerators.eventInfos().toGeneratorOfNonEmptyList().generateOne.toList
      val projectId = projectIds.generateOne
      val sinceDate = EventContentGenerators.eventDates.generateOne.value.truncatedTo(ChronoUnit.SECONDS)

      stubFor {
        get(
          s"/events?since=${param(sinceDate.toString)}&project-id=${projectId.show}&page=1&per_page=35&sort=eventDate%3AASC"
        ).willReturn(ok(events))
      }

      val criteria = SearchCriteria
        .forProject(projectId)
        .withSince(EventDate(sinceDate))
        .withPerPage(35)
        .sortBy(SearchCriteria.Sort.EventDateAsc)

      val result = client.getEvents(criteria).unsafeRunSync()
      result.toEither.fold(throw _, identity) shouldBe events
    }

    "return failure" in {
      stubFor {
        get(s"/events?status=${EventStatus.TriplesStore.value}&page=1")
          .willReturn(serverError())
      }
      val result = client.getEvents(SearchCriteria.forStatus(EventStatus.TriplesStore)).unsafeRunSync()
      result match {
        case Result.Success(_)  => fail(s"unexpected $result result")
        case Result.Unavailable => fail(s"unexpected $result result")
        case Result.Failure(_)  => ()
      }
    }

    "return unavailable" in {
      stubFor {
        get(s"/events?status=${EventStatus.TriplesStore.value}&page=1")
          .willReturn(serviceUnavailable())
      }
      val result = client.getEvents(SearchCriteria.forStatus(EventStatus.TriplesStore)).unsafeRunSync()
      result match {
        case Result.Success(_)  => fail(s"unexpected $result result")
        case Result.Unavailable => ()
        case Result.Failure(_)  => fail(s"unexpected $result result")
      }
    }
  }

  "getEventPayload" should {
    val eventId     = EventsGenerators.eventIds.generateOne
    val projectSlug = GraphModelGenerators.projectSlugs.generateOne
    val gzippedBody = ByteVector.fromValidHex("cafebabe")

    "return bytes" in {
      stubFor {
        get(s"/events/${param(eventId.value)}/${param(projectSlug.value)}/payload")
          .willReturn(
            aResponse()
              .withHeader("Content-Type", "application/gzip")
              .withBody(gzippedBody.toArray)
          )
      }
      val result = client.getEventPayload(eventId, projectSlug).unsafeRunSync()
      result match {
        case Result.Success(Some(r)) => r.data shouldBe gzippedBody
        case _                       => fail(s"unexpected result: $result")
      }
    }

    "return not found" in {
      stubFor {
        get(s"/events/${param(eventId.value)}/${param(projectSlug.value)}/payload")
          .willReturn(notFound())
      }
      val result = client.getEventPayload(eventId, projectSlug).unsafeRunSync()
      result match {
        case Result.Success(None) => ()
        case _                    => fail(s"unexpected result: $result")
      }
    }
  }

  "getStatus" should {

    "return Status for Ok" in {

      val subscriptionCategoryName = categoryNames.generateOne
      val payload = json"""{
        "subscriptions": [
          {
            "categoryName": $subscriptionCategoryName
          }
        ]
      }"""

      stubFor {
        get(s"/status")
          .willReturn(
            okJson(payload.noSpaces)
          )
      }

      client.getStatus.unsafeRunSync().toEither.value shouldBe eventlogapi.ServiceStatus(
        Set(eventlogapi.ServiceStatus.SubscriptionStatus(subscriptionCategoryName, maybeCapacity = None))
      )
    }

    "fail for InternalServerError" in {

      val status = Status.InternalServerError
      stubFor {
        get(s"/status")
          .willReturn(
            aResponse().withStatus(status.code).withBody(jsons.generateOne.spaces2)
          )
      }

      client.getStatus.unsafeRunSync().toEither.left.value shouldBe Result.failure(show"Unexpected response: $status")
    }
  }

  private def ok[A](value: A)(implicit enc: Encoder[A]): ResponseDefinitionBuilder =
    okJson(enc.apply(value).spaces2)

  private def param: String => String =
    UrlEncoder.urlEncode
}

object Http4sEventLogClientSpec {

  /** Encoders as used by the microservice endpoint. */
  object JsonEncoders {
    import io.circe.Json
    import io.circe.literal._
    import io.circe.syntax._

    implicit val statusProcessingTimeEncoder: Encoder[StatusProcessingTime] = { processingTime =>
      json"""{
          "status":         ${processingTime.status},
          "processingTime": ${processingTime.processingTime}
        }"""
    }

    implicit val projectIdsEncoder: Encoder[EventInfo.ProjectIds] = ids =>
      json"""{ "id": ${ids.id}, "slug": ${ids.slug} }"""

    implicit val eventInfoEncoder: Encoder[EventInfo] = eventInfo =>
      json"""{
          "id":              ${eventInfo.eventId},
          "project":     ${eventInfo.project},
          "status":          ${eventInfo.status},
          "processingTimes": ${eventInfo.processingTimes},
          "date" :           ${eventInfo.eventDate},
          "executionDate":   ${eventInfo.executionDate}
        }""".deepMerge(
        eventInfo.maybeMessage.map(m => Json.obj("message" -> m.asJson)).getOrElse(Json.obj())
      )
  }
}
