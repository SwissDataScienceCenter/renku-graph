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

import EventProcessingGenerators._
import cats.MonadError
import cats.data.EitherT
import cats.data.EitherT.{leftT, rightT}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventStatus.New
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.IOCommitEventProcessor.eventsProcessingTimesBuilder
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator.GenerationRecoverableError
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

  import AccessTokenFinder._

  "process" should {

    "succeed if event is successfully turned into triples" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(commitEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      successfulTriplesGeneration(commitEvent -> jsonLDTriples.generateOne)

      eventProcessor.process(commitEvent, schemaVersion) shouldBe ().pure[Try]

      logSummary(commitEvent)

      verifyMetricsCollected()
    }

    "succeed if event fails during triples generation" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(forProjectPath = commitEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commitEvent, maybeAccessToken)
        .returning(EitherT.liftF(exception.raiseError[Try, JsonLDTriples]))

      expectEventMarkedAsNonRecoverableFailure(commitEvent, exception)

      eventProcessor.process(commitEvent, schemaVersion) shouldBe ().pure[Try]

      logError(commitEvent, exception)
    }

    s"succeed and mark event with RecoverableFailure if finding triples fails with $GenerationRecoverableError" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(commitEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = GenerationRecoverableError(nonBlankStrings().generateOne.value)
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commitEvent, maybeAccessToken)
        .returning(leftT[Try, JsonLDTriples](exception))

      expectEventMarkedAsRecoverableFailure(commitEvent, exception)

      eventProcessor.process(commitEvent, schemaVersion) shouldBe ().pure[Try]

      logError(commitEvent, exception, exception.getMessage)
    }

    s"put event into status $New if finding access token fails" in new TestCase {

      val commitEvent = commitEvents.generateOne

      val exception = exceptions.generateOne
      givenFetchingAccessToken(commitEvent.project.path)
        .returning(exception.raiseError[Try, Option[AccessToken]])

      expectEventRolledBackToNew(commitEvent)

      eventProcessor.process(commitEvent, schemaVersion) shouldBe ().pure[Try]

      logger.getMessages(Error).map(_.message) shouldBe List(
        s"${logMessageCommon(commitEvent)}: commit Event processing failure"
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
        logger
      ).unsafeRunSync()
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)
  private lazy val eventsProcessingTimes = eventsProcessingTimesBuilder.create()

  private trait TestCase {

    val maybeAccessToken = Gen.option(accessTokens).generateOne
    val schemaVersion    = projectSchemaVersions.generateOne

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

    def successfulTriplesGeneration(commitAndTriples: (CommitEvent, JsonLDTriples)) = {
      val (commit, triples) = commitAndTriples
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(rightT[Try, ProcessingRecoverableError](triples))

      expectEventMarkedAsTriplesGenerated(CompoundEventId(commit.eventId, commit.project.id),
                                          commit.project.path,
                                          triples
      )
    }

    def expectEventMarkedAsRecoverableFailure(commit: CommitEvent, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(commit.compoundEventId, commit.project.path, EventStatus.GenerationRecoverableFailure, exception)
        .returning(().pure[Try])

    def expectEventMarkedAsNonRecoverableFailure(commit: CommitEvent, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(commit.compoundEventId, commit.project.path, EventStatus.GenerationNonRecoverableFailure, exception)
        .returning(().pure[Try])

    def expectEventMarkedAsTriplesGenerated(compoundEventId: CompoundEventId,
                                            projectPath:     Path,
                                            triples:         JsonLDTriples
    ) =
      (eventStatusUpdater
        .toTriplesGenerated(_: CompoundEventId, _: Path, _: JsonLDTriples, _: SchemaVersion, _: EventProcessingTime))
        .expects(compoundEventId,
                 projectPath,
                 triples,
                 schemaVersion,
                 EventProcessingTime(Duration.ofMillis(singleEventTimeRecorder.elapsedTime.value))
        )
        .returning(().pure[Try])

    def expectEventRolledBackToNew(commit: CommitEvent) =
      (eventStatusUpdater
        .rollback[New](_: CompoundEventId, _: Path)(_: () => New))
        .expects(commit.compoundEventId, commit.project.path, *)
        .returning(().pure[Try])

    def logSummary(commit: CommitEvent) = logger.logged(
      Info(s"${commonLogMessage(commit)} processed in ${allEventsTimeRecorder.elapsedTime}ms")
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
}
