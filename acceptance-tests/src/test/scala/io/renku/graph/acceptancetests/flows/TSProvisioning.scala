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

package io.renku.graph.acceptancetests.flows

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.renku.events.CategoryName
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.db.EventLog
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.acceptancetests.tooling.{ApplicationServices, ModelImplicits}
import io.renku.graph.model.events.{CommitId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.testtools.IOSpec
import io.renku.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.{Assertion, EitherValues}

import java.lang.Thread.sleep
import scala.annotation.tailrec
import scala.concurrent.duration._

trait TSProvisioning
    extends ModelImplicits
    with AccessTokenPresence
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers
    with EitherValues {

  self: ApplicationServices with IOSpec =>

  def `data in the Triples Store`(
      project:     data.Project,
      commitId:    CommitId,
      accessToken: AccessToken
  )(implicit ioRuntime: IORuntime): Assertion =
    `data in the Triples Store`(project, NonEmptyList(commitId, Nil), accessToken)

  def `data in the Triples Store`(
      project:     data.Project,
      commitIds:   NonEmptyList[CommitId],
      accessToken: AccessToken
  )(implicit ioRuntime: IORuntime): Assertion = {

    givenAccessTokenPresentFor(project, accessToken)

    commitIds.toList.foreach { commitId =>
      webhookServiceClient
        .POST("webhooks/events", HookToken(project.id), data.GitLab.pushEvent(project, commitId))
        .status shouldBe Accepted
    }

    // commitId is the eventId
    val condition = commitIds.map(e => EventId(e.value)).toList.map(_ -> EventStatus.TriplesStore)
    waitForAllEvents(project.id, condition: _*)
  }

  def waitForAllEvents(projectId: projects.GitLabId, expect: (EventId, EventStatus)*) = {
    val expectedResult = expect.toSet
    val ids            = expect.map(_._1).toSet

    val findEvents =
      eventLogClient
        .getEvents(Left(projectId))
        .map(list =>
          list
            .filter(ev => ids.contains(ev.id))
            .map(ev => ev.id -> ev.status)
            .toSet
        )

    val waitTimes = fs2.Stream.iterate(1d)(_ * 1.5).map(_.seconds).covary[IO].evalMap(IO.sleep)

    val tries =
      fs2.Stream
        .repeatEval(findEvents)
        .zip(waitTimes)
        .map(_._1)
        .evalTap(result => IO.println(s"Wait for event status: $result -> $expectedResult"))
        .takeThrough(found => found != expectedResult)
        .limit(13)

    val lastValue = tries.compile.lastOrError.unsafeRunSync()
    lastValue shouldBe expectedResult
  }

  def `check hook cannot be found`(projectId: projects.GitLabId, accessToken: AccessToken): Assertion = eventually {
    webhookServiceClient.`GET projects/:id/events/status`(projectId, accessToken).status shouldBe NotFound
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
