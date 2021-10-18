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

package io.renku.graph.acceptancetests.eventlog

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, Json}
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project.Statistics.CommitsCount
import io.renku.graph.acceptancetests.data.{RdfStoreData, dataProjects}
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.acceptancetests.tooling.ResponseTools._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events
import io.renku.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.jsonld.syntax._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class EventsResourceSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreData
    with should.Matchers
    with Eventually
    with AcceptanceTestPatience
    with EntitiesGenerators {

  Feature("GET /events?project-path=<path> to return info about all the project events") {

    Scenario("As a user I would like to see all events from the project with the given path") {
      val commits:              List[events.CommitId] = commitIds.generateNonEmptyList(maxElements = 6).toList
      implicit val accessToken: AccessToken           = accessTokens.generateOne
      val project = dataProjects(projectEntities(anyVisibility), CommitsCount(commits.size)).generateOne

      When("there are no events for the given project in EL")
      Then("the resource should return OK with an empty array")
      val noEventsResponse = eventLogClient.GET(s"events?project-path=${urlEncode(project.path.show)}")
      noEventsResponse.status                    shouldBe Ok
      noEventsResponse.bodyAsJson.as[List[Json]] shouldBe Nil.asRight

      commits foreach { commitId => `data in the RDF store`(project, project.entitiesProject.asJsonLD, commitId) }

      eventually {
        val eventsResponse = eventLogClient.GET(s"events?project-path=${urlEncode(project.path.show)}")
        eventsResponse.status shouldBe Ok
        val Right(events) = eventsResponse.bodyAsJson.as[List[EventInfo]]
        events.size               shouldBe commits.size
        events.map(_.eventId.value) should contain theSameElementsAs commits.map(_.value)
      }
    }
  }

  private case class EventInfo(eventId:         EventId,
                               status:          EventStatus,
                               maybeMessage:    Option[String],
                               processingTimes: List[StatusProcessingTime]
  )
  private case class StatusProcessingTime(status: EventStatus, processingTime: EventProcessingTime)

  private implicit lazy val infoDecoder: Decoder[EventInfo] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    implicit val statusProcessingTimeDecoder: Decoder[StatusProcessingTime] = cursor =>
      for {
        status         <- cursor.downField("status").as[EventStatus]
        processingTime <- cursor.downField("processingTime").as[EventProcessingTime]
      } yield StatusProcessingTime(status, processingTime)

    for {
      id              <- cursor.downField("id").as[EventId]
      status          <- cursor.downField("status").as[EventStatus]
      maybeMessage    <- cursor.downField("message").as[Option[String]]
      processingTimes <- cursor.downField("processingTimes").as[List[StatusProcessingTime]]
    } yield EventInfo(id, status, maybeMessage, processingTimes)
  }
}
