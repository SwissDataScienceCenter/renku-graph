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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration

import cats.data.EitherT
import cats.data.EitherT.{leftT, rightT}
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.events.EventStatus.{FailureStatus, New}
import io.renku.graph.model.events._
import io.renku.graph.model.projects.Path
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.jsonld.JsonLD
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater.{ExecutionDelay, RollbackStatus}
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.EventProcessingGenerators._
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration.TriplesGenerator
import io.renku.triplesgenerator.events.consumers.{EventStatusUpdater, ProcessingRecoverableError}
import io.renku.triplesgenerator.generators.ErrorGenerators.{logWorthyRecoverableErrors, nonRecoverableMalformedRepoErrors, silentRecoverableErrors}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.util.Try

class EventProcessorSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  import AccessTokenFinder.Implicits._

  "process" should {

    "succeed if event is successfully turned into triples" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(commitEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      successfulTriplesGeneration(commitEvent -> jsonLDEntities.generateOne)

      eventProcessor.process(commitEvent) shouldBe ().pure[Try]

      logger.loggedOnly(
        Info(s"${commonLogMessage(commitEvent)} accepted"),
        Info(s"${commonLogMessage(commitEvent)} processed in ${allEventsTimeRecorder.elapsedTime}ms")
      )
    }

    "succeed if event fails during triples generation with a ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(forProjectPath = commitEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = nonRecoverableMalformedRepoErrors.generateOne
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commitEvent, maybeAccessToken)
        .returning(EitherT.liftF(exception.raiseError[Try, JsonLD]))

      expectEventMarkedAsNonRecoverableFailure(commitEvent, exception)

      eventProcessor.process(commitEvent) shouldBe ().pure[Try]
    }

    "log an error and succeed " +
      "if event fails during triples generation with a non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

        val commitEvent = commitEvents.generateOne

        givenFetchingAccessToken(forProjectPath = commitEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val exception = exceptions.generateOne
        (triplesFinder
          .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
          .expects(commitEvent, maybeAccessToken)
          .returning(EitherT.liftF(exception.raiseError[Try, JsonLD]))

        expectEventMarkedAsNonRecoverableFailure(commitEvent, exception)

        eventProcessor.process(commitEvent) shouldBe ().pure[Try]

        logError(commitEvent, exception)
      }

    "mark event as RecoverableFailure and log an error if finding triples fails with LogWorthyRecoverableError" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(commitEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = logWorthyRecoverableErrors.generateOne
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commitEvent, maybeAccessToken)
        .returning(leftT[Try, JsonLD](exception))

      expectEventMarkedAsRecoverableFailure(commitEvent, exception)

      eventProcessor.process(commitEvent) shouldBe ().pure[Try]

      logError(commitEvent, exception, exception.getMessage)
    }

    "mark event as RecoverableFailure and refrain from logging an error " +
      "if finding triples fails with SilentRecoverableError" in new TestCase {

        val commitEvent = commitEvents.generateOne

        givenFetchingAccessToken(commitEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val exception = silentRecoverableErrors.generateOne
        (triplesFinder
          .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
          .expects(commitEvent, maybeAccessToken)
          .returning(leftT[Try, JsonLD](exception))

        expectEventMarkedAsRecoverableFailure(commitEvent, exception)

        eventProcessor.process(commitEvent) shouldBe ().pure[Try]

        logger.getMessages(Error).isEmpty shouldBe true
      }

    s"put event into status $New if finding access token fails" in new TestCase {

      val commitEvent = commitEvents.generateOne

      val exception = exceptions.generateOne
      givenFetchingAccessToken(commitEvent.project.path)
        .returning(exception.raiseError[Try, Option[AccessToken]])

      expectEventRolledBackToNew(commitEvent)

      eventProcessor.process(commitEvent) shouldBe ().pure[Try]

      logger.getMessages(Error).map(_.message) shouldBe List(s"${logMessageCommon(commitEvent)} processing failure")
    }
  }

  private trait TestCase {

    val maybeAccessToken = Gen.option(accessTokens).generateOne

    implicit val logger:            TestLogger[Try]        = TestLogger[Try]()
    implicit val accessTokenFinder: AccessTokenFinder[Try] = mock[AccessTokenFinder[Try]]
    val triplesFinder           = mock[TriplesGenerator[Try]]
    val eventStatusUpdater      = mock[EventStatusUpdater[Try]]
    val allEventsTimeRecorder   = TestExecutionTimeRecorder[Try](maybeHistogram = None)
    val singleEventTimeRecorder = TestExecutionTimeRecorder[Try](maybeHistogram = None)
    val eventProcessor = new EventProcessorImpl(
      triplesFinder,
      eventStatusUpdater,
      allEventsTimeRecorder,
      singleEventTimeRecorder
    )

    def givenFetchingAccessToken(forProjectPath: Path) =
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(forProjectPath, projectPathToPath)

    def successfulTriplesGeneration(commitAndTriples: (CommitEvent, JsonLD)) = {
      val (commit, payload) = commitAndTriples
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(rightT[Try, ProcessingRecoverableError](payload))

      expectEventMarkedAsTriplesGenerated(CompoundEventId(commit.eventId, commit.project.id),
                                          commit.project.path,
                                          payload
      )
    }

    def expectEventMarkedAsRecoverableFailure(commit: CommitEvent, exception: ProcessingRecoverableError) = {
      val executionDelay = exception match {
        case _: SilentRecoverableError    => ExecutionDelay(Duration ofHours 1)
        case _: LogWorthyRecoverableError => ExecutionDelay(Duration ofMinutes 15)
      }
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Path, _: FailureStatus, _: Throwable, _: ExecutionDelay))
        .expects(commit.compoundEventId,
                 commit.project.path,
                 EventStatus.GenerationRecoverableFailure,
                 exception,
                 executionDelay
        )
        .returning(().pure[Try])
    }

    def expectEventMarkedAsNonRecoverableFailure(commit: CommitEvent, exception: Throwable) =
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Path, _: FailureStatus, _: Throwable))
        .expects(commit.compoundEventId, commit.project.path, EventStatus.GenerationNonRecoverableFailure, exception)
        .returning(().pure[Try])

    def expectEventMarkedAsTriplesGenerated(compoundEventId: CompoundEventId, projectPath: Path, payload: JsonLD) =
      (eventStatusUpdater
        .toTriplesGenerated(_: CompoundEventId, _: Path, _: JsonLD, _: EventProcessingTime))
        .expects(compoundEventId,
                 projectPath,
                 payload,
                 EventProcessingTime(Duration.ofMillis(singleEventTimeRecorder.elapsedTime.value))
        )
        .returning(().pure[Try])

    def expectEventRolledBackToNew(commit: CommitEvent) =
      (eventStatusUpdater
        .rollback(_: CompoundEventId, _: Path, _: RollbackStatus.New.type))
        .expects(commit.compoundEventId, commit.project.path, *)
        .returning(().pure[Try])

    def logError(commit: CommitEvent, exception: Exception, message: String = "failed") =
      logger.logged(Error(s"${commonLogMessage(commit)} $message", exception))

    def commonLogMessage(event: CommitEvent): String =
      show"$categoryName: ${event.compoundEventId}, projectPath = ${event.project.path}"
  }
}
