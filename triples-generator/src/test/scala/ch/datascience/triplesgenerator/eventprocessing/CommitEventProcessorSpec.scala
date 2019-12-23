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
import cats.data.EitherT.{leftT, rightT}
import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.{EventBody, EventMessage}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.tokenrepository.IOAccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.interpreters.TestLogger.Matcher.NotRefEqual
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.eventprocessing.IOCommitEventProcessor.eventsProcessingTimes
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.interpreters.TryTriplesCurator
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TryUploader
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.{Assertion, WordSpec}

import scala.collection.JavaConverters._
import scala.util.Try

class CommitEventProcessorSpec extends WordSpec with MockFactory {
  import IOAccessTokenFinder._

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

      logSummary(commits, uploaded = commitsAndTriples.size, failed = 0)

      verifyMetricsCollected()
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
        .returning(EitherT.liftF[Try, GenerationRecoverableError, JsonLDTriples](context.raiseError(exception2)))

      expectEventMarkedFailed(commit2.commitEventId, NonRecoverableFailure, exception2)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commit2, exception2)
      logSummary(commits, uploaded = successfulCommitsAndTriples.size, failed = 1)
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
        .returning(EitherT.liftF[Try, GenerationRecoverableError, JsonLDTriples](context.raiseError(exception)))

      expectEventMarkedFailed(commit.commitEventId, NonRecoverableFailure, exception)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception)
      logSummary(commits, uploaded = 0, failed = 1)
    }

    s"succeed and mark event with $RecoverableFailure if finding triples fails with $GenerationRecoverableError" in new TestCase {

      val commits       = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val exception = GenerationRecoverableError(nonBlankStrings().generateOne.value)
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(leftT[Try, JsonLDTriples](exception))

      expectEventMarkedFailed(commit.commitEventId, RecoverableFailure, exception)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception.message)
      logSummary(commits, uploaded = 0, failed = 1)
    }

    s"succeed and mark event with $NonRecoverableFailure if curating triples fails" in new TestCase {

      val commits       = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val rawTriples = jsonLDTriples.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(rightT[Try, GenerationRecoverableError](rawTriples))

      val exception = exceptions.generateOne
      (triplesCurator
        .curate(_: JsonLDTriples))
        .expects(rawTriples)
        .returning(context.raiseError(exception))

      expectEventMarkedFailed(commit.commitEventId, NonRecoverableFailure, exception)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception)
      logSummary(commits, uploaded = 0, failed = 1)
    }

    s"succeed and mark event with $RecoverableFailure " +
      s"if uploading triples to the dataset fails with $DeliveryFailure for at least one event" in new TestCase {

      val commits                   = commitsLists(size = Gen.const(2)).generateOne
      val commit1 +: commit2 +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      val rawTriples = jsonLDTriples.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit1, maybeAccessToken)
        .returning(rightT[Try, GenerationRecoverableError](rawTriples))

      val curatedTriples = curatedTriplesObjects().generateOne
      (triplesCurator
        .curate(_: JsonLDTriples))
        .expects(rawTriples)
        .returning(context.pure(curatedTriples))

      val uploadingError = nonEmptyStrings().map(DeliveryFailure.apply).generateOne
      (uploader
        .upload(_: CuratedTriples))
        .expects(curatedTriples)
        .returning(context.pure(uploadingError))

      val exception2 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit2, maybeAccessToken)
        .returning(EitherT.liftF[Try, GenerationRecoverableError, JsonLDTriples](context.raiseError(exception2)))

      expectEventMarkedFailed(commit1.commitEventId, RecoverableFailure, uploadingError)

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, uploadingError.message)
      logSummary(commits, uploaded = 0, failed = 2)
    }

    s"succeed and mark event with $NonRecoverableFailure " +
      s"if uploading triples to the store fails with either $InvalidTriplesFailure or $InvalidUpdatesFailure for at least one event" in new TestCase {

      (InvalidTriplesFailure("error") +: InvalidUpdatesFailure("error") +: Nil) foreach { failure =>
        val commits                   = commitsLists(size = Gen.const(2)).generateOne
        val commit1 +: commit2 +: Nil = commits.toList

        (eventsDeserialiser
          .deserialiseToCommitEvents(_: EventBody))
          .expects(eventBody)
          .returning(context.pure(commits))

        givenFetchingAccessToken(forProjectId = commits.head.project.id)
          .returning(context.pure(maybeAccessToken))

        val rawTriples = jsonLDTriples.generateOne
        (triplesFinder
          .generateTriples(_: Commit, _: Option[AccessToken]))
          .expects(commit1, maybeAccessToken)
          .returning(rightT[Try, GenerationRecoverableError](rawTriples))

        val curatedTriples = curatedTriplesObjects().generateOne
        (triplesCurator
          .curate(_: JsonLDTriples))
          .expects(rawTriples)
          .returning(context.pure(curatedTriples))

        (uploader
          .upload(_: CuratedTriples))
          .expects(curatedTriples)
          .returning(context.pure(failure))

        val exception2 = exceptions.generateOne
        (triplesFinder
          .generateTriples(_: Commit, _: Option[AccessToken]))
          .expects(commit2, maybeAccessToken)
          .returning(EitherT.liftF[Try, GenerationRecoverableError, JsonLDTriples](context.raiseError(exception2)))

        expectEventMarkedFailed(commit1.commitEventId, NonRecoverableFailure, failure)

        eventProcessor(eventBody) shouldBe context.unit

        logError(commits.head, failure.getMessage)
        logSummary(commits, uploaded = 0, failed = 2)
        logger.reset()
      }
    }

    s"succeed and log an error if marking event as $TriplesStore fails" in new TestCase {

      val commits       = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.pure(maybeAccessToken))

      successfulTriplesGenerationAndUpload(commit -> jsonLDTriples.generateOne)

      val exception = exceptions.generateOne
      (eventLogMarkDone
        .markEventDone(_: CommitEventId))
        .expects(commit.commitEventId)
        .returning(context.raiseError(exception))

      eventProcessor(eventBody) shouldBe context.unit

      logError(commits.head, exception, s"failed to mark as $TriplesStore in the Event Log")
      logSummary(commits, uploaded = 1, failed = 0)
    }

    "succeed and log an error if CommitEvent deserialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.raiseError(exception))

      eventProcessor(eventBody) shouldBe context.unit

      logger.loggedOnly(Error(s"Commit Event processing failure: $eventBody", exception))
    }

    s"mark event as $New and log an error if finding an access token fails" in new TestCase {

      val commits = commitsLists(size = Gen.const(1)).generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: EventBody))
        .expects(eventBody)
        .returning(context.pure(commits))

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectId = commits.head.project.id)
        .returning(context.raiseError(exception))

      (eventLogMarkNew
        .markEventNew(_: CommitEventId))
        .expects(commits.head.commitEventId)
        .returning(context.unit)

      eventProcessor(eventBody) shouldBe context.unit

      logger.loggedOnly(
        Error(
          message          = s"Commit Event processing failure: $eventBody",
          throwableMatcher = NotRefEqual(new Exception("processing failure -> Event rolled back", exception))
        )
      )
    }
  }

  "eventsProcessingTimes histogram" should {

    "have 'events_processing_times' name" in {
      eventsProcessingTimes.startTimer().observeDuration()

      eventsProcessingTimes.collect().asScala.headOption.map(_.name) shouldBe Some(
        "events_processing_times"
      )
    }

    "be registered in the Metrics Registry" in {
      eventsProcessingTimes.startTimer().observeDuration()
      MetricsRegistry.verifyInRegistry("events_processing_times") shouldBe true
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val eventBody        = eventBodies.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne

    val eventsDeserialiser    = mock[TryCommitEventsDeserialiser]
    val accessTokenFinder     = mock[TryAccessTokenFinder]
    val triplesFinder         = mock[TryTriplesGenerator]
    val triplesCurator        = mock[TryTriplesCurator]
    val uploader              = mock[TryUploader]
    val eventLogMarkDone      = mock[TryEventLogMarkDone]
    val eventLogMarkNew       = mock[TryEventLogMarkNew]
    val eventLogMarkFailed    = mock[TryEventLogMarkFailed]
    val logger                = TestLogger[Try]()
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger, Some(eventsProcessingTimes))
    val eventProcessor = new CommitEventProcessor[Try](
      eventsDeserialiser,
      accessTokenFinder,
      triplesFinder,
      triplesCurator,
      uploader,
      eventLogMarkDone,
      eventLogMarkNew,
      eventLogMarkFailed,
      logger,
      executionTimeRecorder
    )

    def givenFetchingAccessToken(forProjectId: ProjectId) =
      (accessTokenFinder
        .findAccessToken(_: ProjectId)(_: ProjectId => String))
        .expects(forProjectId, projectIdToPath)

    def generateTriples(forCommits: NonEmptyList[Commit]): NonEmptyList[(Commit, JsonLDTriples)] =
      forCommits map (_ -> jsonLDTriples.generateOne)

    def successfulTriplesGenerationAndUpload(commitAndTriples: (Commit, JsonLDTriples)) = {
      val (commit, triples) = commitAndTriples
      (triplesFinder
        .generateTriples(_: Commit, _: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(rightT[Try, GenerationRecoverableError](triples))

      val curatedTriples = curatedTriplesObjects().generateOne
      (triplesCurator
        .curate(_: JsonLDTriples))
        .expects(triples)
        .returning(context.pure(curatedTriples))

      (uploader
        .upload(_: CuratedTriples))
        .expects(curatedTriples)
        .returning(context.pure(DeliverySuccess))
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

    def logSummary(commits: NonEmptyList[Commit], uploaded: Int, failed: Int): Assertion =
      logger.logged(
        Info(
          s"${commonLogMessage(commits.head)} processed in ${executionTimeRecorder.elapsedTime}ms: " +
            s"${commits.size} commits, $uploaded commits uploaded, $failed commits failed"
        )
      )

    def logError(commit: Commit, message: String): Assertion =
      logger.logged(Error(s"${commonLogMessage(commit)} $message"))

    def logError(commit: Commit, exception: Exception, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(commit)} $message", exception))

    def commonLogMessage: Commit => String = {
      case CommitWithoutParent(id, project) =>
        s"Commit Event id: $id, project: ${project.id} ${project.path}"
      case CommitWithParent(id, parentId, project) =>
        s"Commit Event id: $id, project: ${project.id} ${project.path}, parentId: $parentId"
    }
  }

  private def verifyMetricsCollected() =
    eventsProcessingTimes
      .collect()
      .asScala
      .flatMap(_.samples.asScala.map(_.name))
      .exists(_ startsWith "events_processing_times") shouldBe true

  private def commits(commitId: CommitId, project: Project): Gen[Commit] =
    for {
      maybeParentId <- Gen.option(commitIds)
    } yield maybeParentId match {
      case None           => CommitWithoutParent(commitId, project)
      case Some(parentId) => CommitWithParent(commitId, parentId, project)
    }

  private def commitsLists(size: Gen[Int Refined Positive] = positiveInts(max = 5)): Gen[NonEmptyList[Commit]] =
    for {
      commitId <- commitIds
      project  <- projects
      size     <- size
      commits  <- Gen.listOfN(size.value, commits(commitId, project))
    } yield NonEmptyList.fromListUnsafe(commits)
}
