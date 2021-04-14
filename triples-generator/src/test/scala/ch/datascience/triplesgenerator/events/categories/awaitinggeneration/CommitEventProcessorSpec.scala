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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.MonadError
import cats.data.EitherT.{leftT, rightT}
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.ConsumersModelGenerators._
import ch.datascience.events.consumers.Project
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventStatus.New
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEvent.{CommitEventWithParent, CommitEventWithoutParent}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.IOCommitEventProcessor.eventsProcessingTimesBuilder
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.prometheus.client.Histogram
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import scala.util.Try

class CommitEventProcessorSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  import IOAccessTokenFinder._

  "process" should {

    "succeed if events are successfully turned into triples" in new TestCase {

      val commitEvents = commitsLists().generateOne

      givenFetchingAccessToken(forProjectPath = commitEvents.head.project.path)
        .returning(maybeAccessToken.pure[Try])

      val commitsAndTriples = generateTriples(forCommits = commitEvents)

      commitsAndTriples.toList foreach successfulTriplesGeneration

      eventProcessor.process(eventId, commitEvents, schemaVersion) shouldBe context.unit

      logSummary(commitEvents, triplesGenerated = commitsAndTriples.size)

      verifyMetricsCollected()
    }

    "succeed if events are successfully turned into triples " +
      "even if some of them failed in different stages" in new TestCase {

        val commitEvents                         = commitsLists(size = Gen.const(3)).generateOne
        val commit1 +: commit2 +: commit3 +: Nil = commitEvents.toList

        givenFetchingAccessToken(forProjectPath = commitEvents.head.project.path)
          .returning(context.pure(maybeAccessToken))

        val successfulCommitsAndTriples = generateTriples(forCommits = NonEmptyList.of(commit1, commit3))

        successfulCommitsAndTriples.toList foreach successfulTriplesGeneration

        val exception2 = exceptions.generateOne
        (triplesFinder
          .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
          .expects(commit2, maybeAccessToken)
          .returning(EitherT.liftF(exception2.raiseError[Try, JsonLDTriples]))

        expectEventMarkedAsNonRecoverableFailure(commit2.compoundEventId, exception2)

        eventProcessor.process(eventId, commitEvents, schemaVersion) shouldBe context.unit

        logError(commit2, exception2)
        logSummary(commitEvents, triplesGenerated = successfulCommitsAndTriples.size, failed = 1)
      }

    "succeed and mark event with NonRecoverableFailure if finding triples fails" in new TestCase {

      val commitEvents  = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commitEvents.toList

      givenFetchingAccessToken(forProjectPath = commitEvents.head.project.path)
        .returning(context.pure(maybeAccessToken))

      val exception = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(EitherT.liftF(exception.raiseError[Try, JsonLDTriples]))

      expectEventMarkedAsNonRecoverableFailure(commit.compoundEventId, exception)

      eventProcessor.process(eventId, commitEvents, schemaVersion) shouldBe context.unit

      logError(commitEvents.head, exception)
      logSummary(commitEvents, failed = 1)
    }

    s"succeed and mark event with RecoverableFailure if finding triples fails with $GenerationRecoverableError" in new TestCase {

      val commitEvents  = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commitEvents.toList

      givenFetchingAccessToken(forProjectPath = commitEvents.head.project.path)
        .returning(context.pure(maybeAccessToken))

      val exception = GenerationRecoverableError(nonBlankStrings().generateOne.value)
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(leftT[Try, JsonLDTriples](exception))

      expectEventMarkedAsRecoverableFailure(commit.compoundEventId, exception)

      eventProcessor.process(eventId, commitEvents, schemaVersion) shouldBe context.unit

      logError(commitEvents.head, exception, exception.getMessage)
      logSummary(commitEvents, failed = 1)
    }

    s"put event into status $New if finding access token fails" in new TestCase {

      val commitEvents  = commitsLists(size = Gen.const(1)).generateOne
      val commit +: Nil = commitEvents.toList

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectPath = commitEvents.head.project.path)
        .returning(exception.raiseError[Try, Option[AccessToken]])

      expectEventRolledBackToNew(commit.compoundEventId)

      eventProcessor.process(eventId, commitEvents, schemaVersion) shouldBe context.unit

      logger.getMessages(Error).map(_.message) shouldBe List(
        s"${logMessageCommon(commit)}: commit Event processing failure"
      )
    }
  }

  "eventsProcessingTimes histogram" should {

    "have 'triples_generation_processing_times' name" in {
      eventsProcessingTimes.startTimer().observeDuration()

      eventsProcessingTimes.collect().asScala.headOption.map(_.name) shouldBe Some(
        "triples_generation_processing_times"
      )
    }

    "be registered in the Metrics Registry" in {

      val metricsRegistry = mock[MetricsRegistry[IO]]

      (metricsRegistry
        .register[Histogram, Histogram.Builder](_: Histogram.Builder)(_: MonadError[IO, Throwable]))
        .expects(eventsProcessingTimesBuilder, *)
        .returning(IO.pure(eventsProcessingTimes))

      val logger = TestLogger[IO]()
      IOCommitEventProcessor(
        metricsRegistry,
        Throttler.noThrottling,
        new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger)),
        logger
      ).unsafeRunSync()
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)
  private lazy val eventsProcessingTimes = eventsProcessingTimesBuilder.create()

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val eventId          = compoundEventIds.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne
    val schemaVersion    = schemaVersions.generateOne

    val accessTokenFinder       = mock[AccessTokenFinder[Try]]
    val triplesFinder           = mock[TriplesGenerator[Try]]
    val eventStatusUpdater      = mock[EventStatusUpdater[Try]]
    val logger                  = TestLogger[Try]()
    val allEventsTimeRecorder   = TestExecutionTimeRecorder[Try](logger, Option(eventsProcessingTimes))
    val singleEventTimeRecorder = TestExecutionTimeRecorder[Try](logger, maybeHistogram = None)
    val eventProcessor = new CommitEventProcessor(
      accessTokenFinder,
      triplesFinder,
      eventStatusUpdater,
      logger,
      allEventsTimeRecorder,
      singleEventTimeRecorder
    )

    def givenFetchingAccessToken(forProjectPath: Path) =
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(forProjectPath, projectPathToPath)

    def generateTriples(forCommits: NonEmptyList[CommitEvent]): NonEmptyList[(CommitEvent, JsonLDTriples)] =
      forCommits map (_ -> jsonLDTriples.generateOne)

    def successfulTriplesGeneration(commitAndTriples: (CommitEvent, JsonLDTriples)) = {
      val (commit, triples) = commitAndTriples
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(rightT[Try, ProcessingRecoverableError](triples))

      expectEventMarkedAsTriplesGenerated(CompoundEventId(commit.eventId, commit.project.id), triples)
    }

    def expectEventMarkedAsRecoverableFailure(commitEventId: CompoundEventId, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(commitEventId, EventStatus.GenerationRecoverableFailure, exception)
        .returning(context.unit)

    def expectEventMarkedAsNonRecoverableFailure(commitEventId: CompoundEventId, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(commitEventId, EventStatus.GenerationNonRecoverableFailure, exception)
        .returning(context.unit)

    def expectEventMarkedAsTriplesGenerated(compoundEventId: CompoundEventId, triples: JsonLDTriples) =
      (eventStatusUpdater
        .toTriplesGenerated(_: CompoundEventId, _: JsonLDTriples, _: SchemaVersion, _: EventProcessingTime))
        .expects(compoundEventId,
                 triples,
                 schemaVersion,
                 EventProcessingTime(Duration.ofMillis(singleEventTimeRecorder.elapsedTime.value))
        )
        .returning(context.unit)

    def expectEventRolledBackToNew(commitEventId: CompoundEventId) =
      (eventStatusUpdater
        .rollback[New](_: CompoundEventId)(_: () => New))
        .expects(commitEventId, *)
        .returning(context.unit)

    def logSummary(commits: NonEmptyList[CommitEvent], triplesGenerated: Int = 0, failed: Int = 0) =
      logger.logged(
        Info(
          s"${commonLogMessage(commits.head)} processed in ${allEventsTimeRecorder.elapsedTime}ms: " +
            s"${commits.size} commits, $triplesGenerated successfully processed, $failed failed"
        )
      )

    def logError(commit: CommitEvent, exception: Exception, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(commit)} $message", exception))

    def commonLogMessage(event: CommitEvent): String =
      s"$categoryName: Commit Event ${event.compoundEventId}, ${event.project.path}"

    def verifyMetricsCollected() =
      eventsProcessingTimes
        .collect()
        .asScala
        .flatMap(_.samples.asScala.map(_.name))
        .exists(_ startsWith "triples_generation_processing_times") shouldBe true
  }

  private def commits(commitId: CommitId, project: Project): Gen[CommitEvent] =
    for {
      maybeParentId <- Gen.option(commitIds)
    } yield maybeParentId match {
      case None           => CommitEventWithoutParent(EventId(commitId.value), project, commitId)
      case Some(parentId) => CommitEventWithParent(EventId(commitId.value), project, commitId, parentId)
    }

  private def commitsLists(size: Gen[Int Refined Positive] = positiveInts(max = 5)): Gen[NonEmptyList[CommitEvent]] =
    for {
      commitId <- commitIds
      project  <- projects
      size     <- size
      commits  <- Gen.listOfN(size.value, commits(commitId, project))
    } yield NonEmptyList.fromListUnsafe(commits)
}
