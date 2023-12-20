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
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.events.EventStatus.{FailureStatus, New}
import io.renku.graph.model.events._
import io.renku.graph.model.projects.Slug
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.jsonld.JsonLD
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.api.TokenRepositoryClient
import io.renku.triplesgenerator.errors.ErrorGenerators.{logWorthyRecoverableErrors, nonRecoverableMalformedRepoErrors, silentRecoverableErrors}
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.errors.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater.{ExecutionDelay, RollbackStatus}
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.EventProcessingGenerators._
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration.TriplesGenerator
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class EventProcessorSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "process" should {

    "succeed if event is successfully turned into triples" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(commitEvent.project.slug)
        .returning(maybeAccessToken.pure[IO])

      successfulTriplesGeneration(commitEvent -> jsonLDEntities.generateOne)

      eventProcessor.process(commitEvent).unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Info(s"${commonLogMessage(commitEvent)} accepted"),
        Info(s"${commonLogMessage(commitEvent)} processed in ${allEventsTimeRecorder.elapsedTime}ms")
      )
    }

    "succeed if event fails during triples generation with a ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(forProjectSlug = commitEvent.project.slug)
        .returning(maybeAccessToken.pure[IO])

      val exception = nonRecoverableMalformedRepoErrors.generateOne
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commitEvent, maybeAccessToken)
        .returning(EitherT.liftF(exception.raiseError[IO, JsonLD]))

      expectEventMarkedAsNonRecoverableFailure(commitEvent, exception)

      eventProcessor.process(commitEvent).unsafeRunSync() shouldBe ()
    }

    "log an error and succeed " +
      "if event fails during triples generation with a non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

        val commitEvent = commitEvents.generateOne

        givenFetchingAccessToken(forProjectSlug = commitEvent.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val exception = exceptions.generateOne
        (triplesFinder
          .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
          .expects(commitEvent, maybeAccessToken)
          .returning(EitherT.liftF(exception.raiseError[IO, JsonLD]))

        expectEventMarkedAsNonRecoverableFailure(commitEvent, exception)

        eventProcessor.process(commitEvent).unsafeRunSync() shouldBe ()

        logError(commitEvent, exception)
      }

    "mark event as RecoverableFailure and log an error if finding triples fails with LogWorthyRecoverableError" in new TestCase {

      val commitEvent = commitEvents.generateOne

      givenFetchingAccessToken(commitEvent.project.slug)
        .returning(maybeAccessToken.pure[IO])

      val exception = logWorthyRecoverableErrors.generateOne
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commitEvent, maybeAccessToken)
        .returning(leftT[IO, JsonLD](exception))

      expectEventMarkedAsRecoverableFailure(commitEvent, exception)

      eventProcessor.process(commitEvent).unsafeRunSync() shouldBe ()

      logError(commitEvent, exception, exception.getMessage)
    }

    "mark event as RecoverableFailure and refrain from logging an error " +
      "if finding triples fails with SilentRecoverableError" in new TestCase {

        val commitEvent = commitEvents.generateOne

        givenFetchingAccessToken(commitEvent.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val exception = silentRecoverableErrors.generateOne
        (triplesFinder
          .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
          .expects(commitEvent, maybeAccessToken)
          .returning(leftT[IO, JsonLD](exception))

        expectEventMarkedAsRecoverableFailure(commitEvent, exception)

        eventProcessor.process(commitEvent).unsafeRunSync() shouldBe ()

        logger.getMessages(Error).isEmpty shouldBe true
      }

    s"put event into status $New if finding access token fails" in new TestCase {

      val commitEvent = commitEvents.generateOne

      val exception = exceptions.generateOne
      givenFetchingAccessToken(commitEvent.project.slug)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      expectEventRolledBackToNew(commitEvent)

      eventProcessor.process(commitEvent).unsafeRunSync() shouldBe ()

      logger.getMessages(Error).map(_.message) shouldBe List(s"${logMessageCommon(commitEvent)} processing failure")
    }
  }

  private trait TestCase {

    val maybeAccessToken = Gen.option(accessTokens).generateOne

    implicit val logger:  TestLogger[IO]            = TestLogger[IO]()
    private val trClient: TokenRepositoryClient[IO] = mock[TokenRepositoryClient[IO]]
    val triplesFinder           = mock[TriplesGenerator[IO]]
    val eventStatusUpdater      = mock[EventStatusUpdater[IO]]
    val allEventsTimeRecorder   = TestExecutionTimeRecorder[IO](maybeHistogram = None)
    val singleEventTimeRecorder = TestExecutionTimeRecorder[IO](maybeHistogram = None)
    val eventProcessor = new EventProcessorImpl(trClient,
                                                triplesFinder,
                                                eventStatusUpdater,
                                                allEventsTimeRecorder,
                                                singleEventTimeRecorder
    )

    def givenFetchingAccessToken(forProjectSlug: Slug) =
      (trClient
        .findAccessToken(_: Slug))
        .expects(forProjectSlug)

    def successfulTriplesGeneration(commitAndTriples: (CommitEvent, JsonLD)) = {
      val (commit, payload) = commitAndTriples
      (triplesFinder
        .generateTriples(_: CommitEvent)(_: Option[AccessToken]))
        .expects(commit, maybeAccessToken)
        .returning(rightT[IO, ProcessingRecoverableError](payload))

      expectEventMarkedAsTriplesGenerated(CompoundEventId(commit.eventId, commit.project.id),
                                          commit.project.slug,
                                          payload
      )
    }

    def expectEventMarkedAsRecoverableFailure(commit: CommitEvent, exception: ProcessingRecoverableError) = {
      val executionDelay = exception match {
        case _: SilentRecoverableError    => ExecutionDelay(Duration ofHours 1)
        case _: LogWorthyRecoverableError => ExecutionDelay(Duration ofMinutes 15)
      }
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Slug, _: FailureStatus, _: Throwable, _: ExecutionDelay))
        .expects(commit.compoundEventId,
                 commit.project.slug,
                 EventStatus.GenerationRecoverableFailure,
                 exception,
                 executionDelay
        )
        .returning(().pure[IO])
    }

    def expectEventMarkedAsNonRecoverableFailure(commit: CommitEvent, exception: Throwable) =
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Slug, _: FailureStatus, _: Throwable))
        .expects(commit.compoundEventId, commit.project.slug, EventStatus.GenerationNonRecoverableFailure, exception)
        .returning(().pure[IO])

    def expectEventMarkedAsTriplesGenerated(compoundEventId: CompoundEventId, projectSlug: Slug, payload: JsonLD) =
      (eventStatusUpdater
        .toTriplesGenerated(_: CompoundEventId, _: Slug, _: JsonLD, _: EventProcessingTime))
        .expects(compoundEventId,
                 projectSlug,
                 payload,
                 EventProcessingTime(Duration.ofMillis(singleEventTimeRecorder.elapsedTime.value))
        )
        .returning(().pure[IO])

    def expectEventRolledBackToNew(commit: CommitEvent) =
      (eventStatusUpdater
        .rollback(_: CompoundEventId, _: Slug, _: RollbackStatus.New.type))
        .expects(commit.compoundEventId, commit.project.slug, *)
        .returning(().pure[IO])

    def logError(commit: CommitEvent, exception: Exception, message: String = "failed") =
      logger.logged(Error(s"${commonLogMessage(commit)} $message", exception))

    def commonLogMessage(event: CommitEvent): String =
      show"$categoryName: ${event.compoundEventId}, projectSlug = ${event.project.slug}"
  }
}
