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

package io.renku.graph.acceptancetests
package flows

import cats.Show
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import fs2.Stream
import io.renku.eventlog.events.producers.membersync.{categoryName => memberSyncCategory}
import io.renku.eventlog.events.producers.minprojectinfo.{categoryName => minProjectInfoCategory}
import io.renku.events.CategoryName
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.db.{EventLog, TriplesStore}
import io.renku.graph.acceptancetests.testing.AcceptanceTestPatience
import io.renku.graph.model.events.{CommitId, EventId, EventStatus, EventStatusProgress}
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.{Assertion, EitherValues}
import org.typelevel.log4cats.Logger
import tooling.EventLogClient.ProjectEvent
import tooling.{AcceptanceSpec, ApplicationServices, ModelImplicits}

import scala.concurrent.duration._

trait TSProvisioning
    extends ModelImplicits
    with AccessTokenPresence
    with Eventually
    with AcceptanceTestPatience
    with should.Matchers
    with EitherValues {

  self: ApplicationServices with AcceptanceSpec with IOSpec =>

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
    waitForSyncEvents(project.id, memberSyncCategory)
    waitForProjectAuthData(project.slug)
  }

  private def projectEvents(projectId: projects.GitLabId): Stream[IO, List[ProjectEvent]] = {
    val findEvents =
      eventLogClient
        .getEvents(Left(projectId))

    val waitTimes = Stream.iterate(1d)(_ * 1.5).map(_.seconds).covary[IO].evalMap(IO.sleep)

    Stream
      .repeatEval(findEvents)
      .zip(waitTimes)
      .map(_._1)
  }

  def waitForAllEvents(projectId: projects.GitLabId, expect: (EventId, EventStatus)*) = {
    val expectedResult = expect.toSet
    val ids            = expect.map(_._1).toSet

    implicit val showTuple: Show[(EventId, EventStatus)] =
      Show.show { case (id, status) => s"${id.value.take(7)}:$status" }

    implicit val showTuples: Show[Iterable[(EventId, EventStatus)]] =
      Show.show(_.toList.mkString_(", "))

    val tries =
      projectEvents(projectId)
        .map(_.filter(ev => ids.contains(ev.id)).map(ev => ev.id -> ev.status).toSet)
        .evalTap(result => Logger[IO].info(show"Waiting for events on $projectId: $result to match $expectedResult"))
        .takeThrough(found => found != expectedResult)
        .take(15)

    val lastValue = tries.compile.lastOrError.unsafeRunSync()
    lastValue shouldBe expectedResult
  }

  def waitForAllEventsInFinalState(projectId: projects.GitLabId) = {
    val tries =
      projectEvents(projectId)
        .map(_.map(ev => EventStatusProgress.Stage(ev.status)).toSet)
        .evalTap(stages =>
          Logger[IO].info(show"Waiting for the final processing stage on $projectId, currently: $stages")
        )
        .takeThrough(stages => stages.exists(_ != EventStatusProgress.Stage.Final))
        .take(15)

    val lastValue = tries.compile.lastOrError.unsafeRunSync()
    lastValue.forall(_ == EventStatusProgress.Stage.Final) shouldBe true
  }

  def getSyncEvents(projectId: projects.GitLabId) = {
    val getSyncEvents = EventLog.findSyncEventsIO(projectId)

    val waitTimes = Stream.iterate(1d)(_ * 1.5).map(_.seconds).covary[IO].evalMap(IO.sleep)
    Stream
      .repeatEval(getSyncEvents)
      .zip(waitTimes)
      .map(_._1)
  }

  def waitForSyncEvents(projectId: projects.GitLabId, category1: CategoryName, categoryN: CategoryName*) = {
    val expected = categoryN.toSet + category1

    val tries =
      getSyncEvents(projectId)
        .evalTap(l => Logger[IO].info(s"Sync events for project $projectId: ${l.mkString(", ")}"))
        .takeThrough(evs => expected.intersect(evs.toSet) != expected)
        .take(13)

    val lastValue = tries.compile.lastOrError.unsafeRunSync()
    expected.intersect(lastValue.toSet) shouldBe expected
  }

  def getProjectAuthData(slug: projects.Slug) = {
    implicit val sqtr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    val waitTimes = Stream.iterate(1d)(_ * 1.5).map(_.seconds).covary[IO].evalMap(IO.sleep)

    Stream
      .repeatEval(TriplesStore.findProjectAuth(slug))
      .zip(waitTimes)
      .map(_._1)
  }

  def waitForProjectAuthData(slug: projects.Slug) = {
    val tries =
      getProjectAuthData(slug)
        .evalTap {
          case None           => Logger[IO].info(show"auth data not ready for $slug")
          case Some(authData) => Logger[IO].info(show"auth data ready $authData")
        }
        .takeThrough(_.isEmpty)
        .take(15)

    val lastValue = tries.compile.lastOrError.unsafeRunSync()
    lastValue.isDefined shouldBe true
  }

  def `check hook cannot be found`(projectId: projects.GitLabId, accessToken: AccessToken): Assertion = eventually {
    webhookServiceClient.`GET projects/:id/events/status`(projectId, accessToken).status shouldBe NotFound
  }

  def `wait for the Fast Tract event`(projectId: projects.GitLabId) =
    waitForSyncEvents(projectId, minProjectInfoCategory)
}
