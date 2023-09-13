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

package io.renku.triplesgenerator.events.consumers.triplesgenerated

import CategoryGenerators._
import cats.data.EitherT
import cats.data.EitherT.leftT
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities.Project
import io.renku.graph.model.events.EventStatus.{FailureStatus, TransformationNonRecoverableFailure, TransformationRecoverableFailure}
import io.renku.graph.model.events._
import io.renku.graph.model.projects.Slug
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.interpreters.TestLogger.Matcher.NotRefEqual
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.errors.ProcessingRecoverableError.{LogWorthyRecoverableError, SilentRecoverableError}
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater.{ExecutionDelay, RollbackStatus}
import io.renku.triplesgenerator.errors.ErrorGenerators.{logWorthyRecoverableErrors, nonRecoverableMalformedRepoErrors, silentRecoverableErrors}
import io.renku.triplesgenerator.tsprovisioning.TSProvisioner
import io.renku.triplesgenerator.tsprovisioning.triplesuploading.TriplesUploadResult
import io.renku.triplesgenerator.tsprovisioning.triplesuploading.TriplesUploadResult._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class EventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  import AccessTokenFinder.Implicits._

  "process" should {

    "succeed if events are successfully turned into triples" in new TestCase {

      givenFetchingAccessToken(forProjectSlug = event.project.slug)
        .returning(maybeAccessToken.pure[IO])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = EitherT.rightT(project))

      givenSuccessfulTSProvisioning(project)

      expectEventMarkedAsDone(event.compoundEventId, event.project.slug)

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logger.logged(Info(show"$categoryName: $event accepted"))
      logSummary(event, isSuccessful = true)
    }

    "mark event as TransformationRecoverableFailure and log an error " +
      s"if entity building fails with $LogWorthyRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val processingError = logWorthyRecoverableErrors.generateOne
        givenEntityBuilding(event, returning = leftT(processingError))

        expectEventMarkedAsRecoverableFailure(event, processingError)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logError(event, processingError, processingError.getMessage)
        logSummary(event, isSuccessful = false)
      }

    "mark event as TransformationRecoverableFailure and refrain from logging an error " +
      s"if entity building fails with $SilentRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val processingError = silentRecoverableErrors.generateOne
        givenEntityBuilding(event, returning = leftT(processingError))

        expectEventMarkedAsRecoverableFailure(event, processingError)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logSummary(event, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure and log an error " +
      "if entity building fails with a non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val exception = exceptions.generateOne
        givenEntityBuilding(event,
                            returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[IO, Project])
        )

        expectEventMarkedAsNonRecoverableFailure(event, exception)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logError(event, exception, exception.getMessage)
        logSummary(event, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure and refrain from logging an error " +
      "if entity building fails with ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val exception = nonRecoverableMalformedRepoErrors.generateOne
        givenEntityBuilding(event,
                            returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[IO, Project])
        )

        expectEventMarkedAsNonRecoverableFailure(event, exception)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logSummary(event, isSuccessful = false)
        logger.expectNoLogs(severity = Error)
      }

    "mark event with TransformationRecoverableFailure and log an error " +
      "if transforming triples fails with a LogWorthyRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(event, returning = EitherT.rightT(project))

        val processingRecoverableError = logWorthyRecoverableErrors.generateOne
        val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
        givenTSProvisioning(project, returning = failure.pure[IO])

        expectEventMarkedAsRecoverableFailure(event, failure.error)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logError(event, failure.error, failure.message)
        logSummary(event, isSuccessful = false)
      }

    "mark event with TransformationRecoverableFailure and refrain from log an error " +
      "if transforming triples fails with a SilentRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(event, returning = EitherT.rightT(project))

        val processingRecoverableError = silentRecoverableErrors.generateOne
        val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
        givenTSProvisioning(project, returning = failure.pure[IO])

        expectEventMarkedAsRecoverableFailure(event, failure.error)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logSummary(event, isSuccessful = false)
      }

    "mark event with TransformationRecoverableFailure if transforming triples fails with SilentRecoverableError " +
      "but doesn't log any errors" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(event, returning = EitherT.rightT(project))

        val failure =
          TriplesUploadResult.RecoverableFailure(SilentRecoverableError(exceptions.generateOne.getMessage))
        givenTSProvisioning(project, returning = failure.pure[IO])

        expectEventMarkedAsRecoverableFailure(event, failure.error)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logger.getMessages(Error).isEmpty shouldBe true
        logSummary(event, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure if transforming triples fails" in new TestCase {

      givenFetchingAccessToken(forProjectSlug = event.project.slug)
        .returning(maybeAccessToken.pure[IO])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = EitherT.rightT(project))

      val exception = exceptions.generateOne
      givenTSProvisioning(project, returning = exception.raiseError[IO, TriplesUploadResult])

      expectEventMarkedAsNonRecoverableFailure(event, exception)

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, exception, exception.getMessage)
      logSummary(event, isSuccessful = false)
    }

    "mark event with TransformationRecoverableFailure " +
      "if uploading triples fails with RecoverableFailure" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(event, returning = EitherT.rightT(project))

        val uploadingError =
          nonEmptyStrings().map(message => RecoverableFailure(LogWorthyRecoverableError(message))).generateOne
        givenTSProvisioning(project, returning = uploadingError.pure[IO])

        expectEventMarkedAsRecoverableFailure(event, uploadingError.error)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logError(event, uploadingError.error, uploadingError.message)
        logSummary(event, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure " +
      "if uploading triples to the store fails with either NonRecoverableFailure" in new TestCase {

        givenFetchingAccessToken(forProjectSlug = event.project.slug)
          .returning(maybeAccessToken.pure[IO])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(event, returning = EitherT.rightT(project))

        val failure = NonRecoverableFailure("error")
        givenTSProvisioning(project, returning = failure.pure[IO])

        expectEventMarkedAsNonRecoverableFailure(event, failure)

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logError(event, failure, failure.message)
        logSummary(event, isSuccessful = false)
      }

    "succeed and log an error if marking event as TriplesStore fails" in new TestCase {

      givenFetchingAccessToken(forProjectSlug = event.project.slug)
        .returning(maybeAccessToken.pure[IO])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = EitherT.rightT(project))

      givenSuccessfulTSProvisioning(project)

      val exception = exceptions.generateOne
      (eventStatusUpdater
        .toTriplesStore(_: CompoundEventId, _: projects.Slug, _: EventProcessingTime))
        .expects(
          event.compoundEventId,
          event.project.slug,
          EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(exception.raiseError[IO, Unit])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, exception, "failed to mark done in the Event Log")
      logSummary(event, isSuccessful = true)
    }

    "mark event as TriplesGenerated and log an error if finding an access token fails" in new TestCase {

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectSlug = event.project.slug)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      expectEventRolledBackToTriplesGenerated(event)

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logger.logged(
        Error(
          message = show"$categoryName: $event processing failure",
          throwableMatcher = NotRefEqual(new Exception("transformation failure -> Event rolled back", exception))
        )
      )
    }
  }

  private trait TestCase {

    val event = triplesGeneratedEvents.generateOne

    implicit val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne

    implicit val logger:            TestLogger[IO]        = TestLogger[IO]()
    implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
    val tsProvisioner         = mock[TSProvisioner[IO]]
    val eventStatusUpdater    = mock[EventStatusUpdater[IO]]
    val entityBuilder         = mock[EntityBuilder[IO]]
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](maybeHistogram = None)
    val eventProcessor =
      new EventProcessorImpl[IO](tsProvisioner, eventStatusUpdater, entityBuilder, executionTimeRecorder)

    def givenFetchingAccessToken(forProjectSlug: Slug) =
      (accessTokenFinder
        .findAccessToken(_: Slug)(_: Slug => String))
        .expects(forProjectSlug, projectSlugToPath)

    def givenSuccessfulTSProvisioning(project: Project) =
      givenTSProvisioning(project, returning = DeliverySuccess.pure[IO])

    def givenTSProvisioning(project: Project, returning: IO[TriplesUploadResult]) =
      (tsProvisioner.provisionTS _)
        .expects(project)
        .returning(returning)

    def givenEntityBuilding(event: TriplesGeneratedEvent, returning: EitherT[IO, ProcessingRecoverableError, Project]) =
      (entityBuilder
        .buildEntity(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(returning)

    def expectEventMarkedAsRecoverableFailure(event: TriplesGeneratedEvent, exception: ProcessingRecoverableError) = {
      val executionDelay = exception match {
        case _: SilentRecoverableError    => ExecutionDelay(Duration.ofHours(1))
        case _: LogWorthyRecoverableError => ExecutionDelay(Duration.ofMinutes(5))
      }
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Slug, _: FailureStatus, _: Throwable, _: ExecutionDelay))
        .expects(event.compoundEventId, event.project.slug, TransformationRecoverableFailure, exception, executionDelay)
        .returning(().pure[IO])
    }

    def expectEventMarkedAsNonRecoverableFailure(event: TriplesGeneratedEvent, exception: Throwable) =
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Slug, _: FailureStatus, _: Throwable))
        .expects(event.compoundEventId, event.project.slug, TransformationNonRecoverableFailure, exception)
        .returning(().pure[IO])

    def expectEventMarkedAsDone(compoundEventId: CompoundEventId, projectSlug: projects.Slug) =
      (eventStatusUpdater
        .toTriplesStore(_: CompoundEventId, _: projects.Slug, _: EventProcessingTime))
        .expects(compoundEventId,
                 projectSlug,
                 EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(().pure[IO])

    def expectEventRolledBackToTriplesGenerated(event: TriplesGeneratedEvent) =
      (eventStatusUpdater
        .rollback(_: CompoundEventId, _: projects.Slug, _: RollbackStatus.TriplesGenerated.type))
        .expects(event.compoundEventId, event.project.slug, *)
        .returning(().pure[IO])

    def logSummary(triplesGeneratedEvent: TriplesGeneratedEvent, isSuccessful: Boolean): Assertion =
      logger.logged(
        Info(
          s"${commonLogMessage(triplesGeneratedEvent)} processed in ${executionTimeRecorder.elapsedTime}ms: " +
            s"${if (isSuccessful) "success" else "failure"}"
        )
      )

    def logError(event: TriplesGeneratedEvent, exception: Throwable, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(event)} $message", NotRefEqual(exception)))

    private def commonLogMessage(event: TriplesGeneratedEvent): String =
      s"$categoryName: ${event.compoundEventId}, projectSlug = ${event.project.slug}"
  }
}
