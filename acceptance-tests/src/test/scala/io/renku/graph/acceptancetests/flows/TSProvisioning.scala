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

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import io.renku.events.CategoryName
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.db.EventLog
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.ServiceClient.ClientResponse
import io.renku.graph.acceptancetests.tooling.{ApplicationServices, ModelImplicits}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.testtools.IOSpec
import io.renku.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should

import java.lang.Thread.sleep
import scala.annotation.tailrec
import scala.concurrent.duration._

trait TSProvisioning
    extends ModelImplicits
    with AccessTokenPresence
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers {
  self: ApplicationServices with IOSpec =>

  def `data in the Triples Store`(
      project:          data.Project,
      commitId:         CommitId = commitIds.generateOne,
      accessToken:      AccessToken
  )(implicit ioRuntime: IORuntime): Assertion =
    `data in the Triples Store`(project, NonEmptyList(commitId, Nil), accessToken)

  def `data in the Triples Store`(
      project:          data.Project,
      commitIds:        NonEmptyList[CommitId],
      accessToken:      AccessToken
  )(implicit ioRuntime: IORuntime): Assertion = {

    givenAccessTokenPresentFor(project, accessToken)

    commitIds.toList.foreach { commitId =>
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted

      sleep((3 second).toMillis)
    }

    `wait for events to be processed`(project.id)
  }

  def `wait for events to be processed`(projectId: projects.GitLabId)(implicit ioRuntime: IORuntime): Assertion = eventually {
    val ClientResponse(status, jsonBody, _) = eventLogClient.fetchProcessingStatus(projectId)
    status                                            shouldBe Ok
    jsonBody.hcursor.downField("progress").as[Double] shouldBe Right(100d)
  }

  def `wait for the Fast Tract event`(projectId: projects.GitLabId)(implicit ioRuntime: IORuntime): Unit = eventually {

    val sleepTime = 1 second

    @tailrec
    def checkIfWasSent(categoryName: CategoryName, attempt: Int = 1): Unit = {
      if (attempt > 20) fail(s"'$categoryName' event wasn't sent after ${(sleepTime * attempt).toSeconds}")

      if (!EventLog.findSyncEvents(projectId).contains(categoryName)) {
        sleep(sleepTime.toMillis)
        checkIfWasSent(categoryName)
      }
    }

    checkIfWasSent(CategoryName("ADD_MIN_PROJECT_INFO"))
  }
}
