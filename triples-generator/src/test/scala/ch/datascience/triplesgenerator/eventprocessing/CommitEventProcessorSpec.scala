/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.EventBody
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, Project, ProjectId}
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class CommitEventProcessorSpec extends WordSpec with MockFactory {

  "apply" should {

    "succeed if a commit event in Json can be deserialised, turn into triples and all stored in Jena successfully" in new TestCase {

      val commits = commitsLists().generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      val maybeAccessToken = None
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      commits map succeedTriplesAndUploading(maybeAccessToken)

      eventProcessor(eventBody) shouldBe context.unit

      logNoAccessTokenMessage(commits.head)
      commits.toList foreach logSuccess
    }

    "succeed if a commit event in Json can be deserialised, turn into triples and all stored in Jena successfully " +
      "even if some failed in different stages" in new TestCase {

      val commits                                         = commitsLists(size = Gen.const(4)).generateOne
      val commit1 +: commit2 +: commit3 +: commit4 +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      val maybeAccessToken = None
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      succeedTriplesAndUploading(maybeAccessToken)(commit1)

      val exception2 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit2, maybeAccessToken)
        .returning(context.raiseError(exception2))

      val triples3   = rdfTriplesSets.generateOne
      val exception3 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit3, maybeAccessToken)
        .returning(context.pure(triples3))
      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples3)
        .returning(context.raiseError(exception3))

      succeedTriplesAndUploading(maybeAccessToken)(commit4)

      eventProcessor(eventBody) shouldBe context.unit

      logNoAccessTokenMessage(commit1)
      logSuccess(commit1)
      logError(commit2, exception2)
      logError(commit3, exception3)
      logSuccess(commit4)
    }

    "succeed and do not log token not found message if an access token was found" in new TestCase {

      val commits = commitsLists().generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      val maybeAccessToken = Some(accessTokens.generateOne)
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      commits map succeedTriplesAndUploading(maybeAccessToken)

      eventProcessor(eventBody) shouldBe context.unit

      notLogAccessTokenMessage(commits.head)
      commits.toList foreach logSuccess
    }

    "succeed but log an error if CommitEvent processing fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.raiseError(exception))

      eventProcessor(eventBody) shouldBe context.unit

      logger.loggedOnly(Error(s"Commit Event processing failure: $eventBody", exception))
    }

    "succeed but log an error if finding an access token fails" in new TestCase {

      val commits = commitsLists(size = Gen.const(1)).generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(commits.head.project.id)
        .returning(context.raiseError(exception))

      eventProcessor(eventBody) shouldBe context.unit

      logger.loggedOnly(Error(s"Commit Event processing failure: $eventBody", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val eventBody = eventBodies.generateOne

    val eventsDeserialiser = mock[TryCommitEventsDeserialiser]
    val accessTokenFinder  = mock[TryAccessTokenFinder]
    val triplesFinder      = mock[TryTriplesFinder]
    val fusekiConnector    = mock[TryFusekiConnector]
    val logger             = TestLogger[Try]()
    val eventProcessor = new CommitEventProcessor[Try](
      eventsDeserialiser,
      accessTokenFinder,
      triplesFinder,
      fusekiConnector,
      logger
    )

    def succeedTriplesAndUploading(maybeAccessToken: Option[AccessToken])(commit: Commit): Unit = {
      val triples = rdfTriplesSets.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(context.pure(triples))

      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples)
        .returning(context.unit)
    }

    def logNoAccessTokenMessage(commit: Commit): Unit =
      logger.logged(Info(s"${commonLogMessage(commit)} no access token found so assuming public project"))

    def notLogAccessTokenMessage(commit: Commit): Unit =
      logger.notLogged(Info(s"${commonLogMessage(commit)} no access token found so assuming public project"))

    def logSuccess(commit: Commit): Unit =
      logger.logged(Info(s"${commonLogMessage(commit)} processed"))

    def logError(commit: Commit, exception: Exception): Unit =
      logger.logged(Error(s"${commonLogMessage(commit)} failed", exception))

    def commonLogMessage: Commit => String = {
      case CommitWithoutParent(id, project) =>
        s"Commit Event id: $id, project: ${project.id} ${project.path}"
      case CommitWithParent(id, parentId, project) =>
        s"Commit Event id: $id, project: ${project.id} ${project.path}, parentId: $parentId"
    }
  }

  private def commits(commitId: CommitId, project: Project): Gen[Commit] =
    for {
      maybeParentId <- Gen.option(commitIds)
    } yield
      maybeParentId match {
        case None           => CommitWithoutParent(commitId, project)
        case Some(parentId) => CommitWithParent(commitId, parentId, project)
      }

  private def commitsLists(size: Gen[Int] = positiveInts(max = 5)): Gen[NonEmptyList[Commit]] =
    for {
      commitId <- commitIds
      project  <- projects
      size     <- size
      commits  <- Gen.listOfN(size, commits(commitId, project))
    } yield NonEmptyList.fromListUnsafe(commits)
}
