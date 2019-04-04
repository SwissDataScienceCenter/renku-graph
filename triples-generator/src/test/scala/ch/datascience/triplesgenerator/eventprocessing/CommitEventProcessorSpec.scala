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
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.{EventBody, EventMessage}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class CommitEventProcessorSpec extends WordSpec with MockFactory {

  "apply" should {

    "succeed if a Commit Event can be deserialised, turned into triples and all stored in Jena successfully" in new TestCase {

      val commits = commitsLists().generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val commitsAndTriples = generateTriples(forCommits = commits)

      commitsAndTriples.toList foreach successfulTriplesGenerationAndUpload

      expectEventMarkedDone(commits.head.commitEventId)

      eventProcessor(eventBody) shouldBe context.unit

      logSummary(commits, triples = commitsAndTriples.map(_._2).toList, uploaded = commitsAndTriples.size, failed = 0)
    }

    "succeed if a Commit Event can be deserialised, turned into triples and all stored in Jena successfully " +
      "even if some failed in different stages" in new TestCase {

      val commits                              = commitsLists(size = Gen.const(3)).generateOne
      val commit1 +: commit2 +: commit3 +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val successfulCommitsAndTriples = generateTriples(forCommits = NonEmptyList.of(commit1, commit3))

      successfulCommitsAndTriples.toList foreach successfulTriplesGenerationAndUpload

      val exception2 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit2, maybeAccessToken)
        .returning(context.raiseError(exception2))

      expectEventMarkedFailed(commit2.commitEventId, NonRecoverableFailure, exception2)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commit2, exception2)
      logSummary(commits,
                 triples  = successfulCommitsAndTriples.map(_._2).toList,
                 uploaded = successfulCommitsAndTriples.size,
                 failed   = 1)
    }

    s"succeed and mark event with $NonRecoverableFailure if finding triples fails" in new TestCase {

      val commits       = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val exception = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(context.raiseError(exception))

      expectEventMarkedFailed(commit.commitEventId, NonRecoverableFailure, exception)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception)
      logSummary(commits, triples = List.empty, uploaded = 0, failed = 1)
    }

    s"succeed and mark event with $TriplesStoreFailure if uploading triples to dataset fails " +
      "for at least one event" in new TestCase {

      val commits                   = commitsLists(size = Gen.const(2)).generateOne
      val commit1 +: commit2 +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val triples1 = rdfTriplesSets.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit1, maybeAccessToken)
        .returning(context.pure(triples1))

      val exception1 = exceptions.generateOne
      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples1)
        .returning(context.raiseError(exception1))

      val exception2 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit2, maybeAccessToken)
        .returning(context.raiseError(exception2))

      expectEventMarkedFailed(commit1.commitEventId, TriplesStoreFailure, exception1)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception1)
      logSummary(commits, triples = List.empty, uploaded = 0, failed = 2)
    }

    s"succeed and log error if marking event in as $TriplesStore fails" in new TestCase {

      val commits       = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val triples = rdfTriplesSets.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(context.pure(triples))

      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples)
        .returning(context.unit)

      val exception = exceptions.generateOne
      (eventLogMarkDone
        .markEventDone(_: CommitEventId))
        .expects(commit.commitEventId)
        .returning(context.raiseError(exception))

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception, s"failed to mark as $TriplesStore in the Event Log")
      logSummary(commits, triples = List(triples), uploaded = 1, failed = 0)
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
      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.raiseError(exception))

      eventProcessor(eventBody) shouldBe context.unit

      logger.loggedOnly(Error(s"Commit Event processing failure: $eventBody", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val eventBody        = eventBodies.generateOne
    val elapsedTime      = elapsedTimes.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne

    val eventsDeserialiser    = mock[TryCommitEventsDeserialiser]
    val accessTokenFinder     = mock[TryAccessTokenFinder]
    val triplesFinder         = mock[TryTriplesFinder]
    val fusekiConnector       = mock[TryFusekiConnector]
    val eventLogMarkDone      = mock[TryEventLogMarkDone]
    val eventLogMarkFailed    = mock[TryEventLogMarkFailed]
    val logger                = TestLogger[Try]()
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](expected = elapsedTime)
    val eventProcessor = new CommitEventProcessor[Try](
      eventsDeserialiser,
      accessTokenFinder,
      triplesFinder,
      fusekiConnector,
      eventLogMarkDone,
      eventLogMarkFailed,
      logger,
      executionTimeRecorder
    )

    def givenFetchingAccessToken(forProjectId: ProjectId) =
      (accessTokenFinder
        .findAccessToken(_: ProjectId))
        .expects(forProjectId)

    def generateTriples(forCommits: NonEmptyList[Commit]): NonEmptyList[(Commit, RDFTriples)] =
      forCommits map (_ -> rdfTriplesSets.generateOne)

    def successfulTriplesGenerationAndUpload(commitAndTriples: (Commit, RDFTriples)): Unit = {
      val (commit, triples) = commitAndTriples
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(context.pure(triples))

      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples)
        .returning(context.unit)
    }

    def expectEventMarkedDone(commitEventId: CommitEventId) =
      (eventLogMarkDone
        .markEventDone(_: CommitEventId))
        .expects(commitEventId)
        .returning(context.unit)

    def expectEventMarkedFailed(commitEventId: CommitEventId, status: FailureStatus, exception: Throwable) =
      (eventLogMarkFailed
        .markEventFailed(_: CommitEventId, _: FailureStatus, _: Option[EventMessage]))
        .expects(commitEventId, status, EventMessage(exception))
        .returning(context.unit)

    def logSummary(commits: NonEmptyList[Commit], triples: List[RDFTriples], uploaded: Int, failed: Int): Unit = {
      val totalTriplesNumber = triples.map(_.value.size()).sum
      logger.logged(
        Info(
          s"${commonLogMessage(commits.head)} processed in ${elapsedTime}ms: " +
            s"${commits.size} commits, $totalTriplesNumber triples in total, $uploaded commits uploaded, $failed commits failed"
        )
      )
    }

    def logError(commit: Commit, exception: Exception, message: String = "failed"): Unit =
      logger.logged(Error(s"${commonLogMessage(commit)} $message", exception))

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
