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

package io.renku.graph.acceptancetests.flows

import cats.effect.unsafe.IORuntime
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.ServiceClient.ClientResponse
import io.renku.graph.acceptancetests.tooling.{GraphServices, ModelImplicits}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should

import java.lang.Thread.sleep
import scala.concurrent.duration._
import cats.data.NonEmptyList

trait RdfStoreProvisioning
    extends ModelImplicits
    with AccessTokenPresence
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers {
  self: GraphServices =>

  def `data in the RDF store`(
      project:            data.Project,
      commitId:           CommitId = commitIds.generateOne
  )(implicit accessToken: AccessToken, ioRuntime: IORuntime): Assertion =
    `data in the RDF store`(project, NonEmptyList(commitId, Nil))

  def `data in the RDF store`(
      project:            data.Project,
      commitIds:          NonEmptyList[CommitId]
  )(implicit accessToken: AccessToken, ioRuntime: IORuntime): Assertion = {

    givenAccessTokenPresentFor(project)

    commitIds.toList.foreach { commitId =>
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      sleep((3 second).toMillis)
    }

    `wait for events to be processed`(project.id)
  }

  def `wait for events to be processed`(projectId: projects.Id)(implicit ioRuntime: IORuntime): Assertion = eventually {
    val ClientResponse(status, jsonBody, _) = eventLogClient.fetchProcessingStatus(projectId)
    status                                            shouldBe Ok
    jsonBody.hcursor.downField("progress").as[Double] shouldBe Right(100d)
  }
}
