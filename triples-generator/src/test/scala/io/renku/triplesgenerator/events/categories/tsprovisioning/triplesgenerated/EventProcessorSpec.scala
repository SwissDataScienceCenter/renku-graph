/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories
package tsprovisioning
package triplesgenerated

import CategoryGenerators._
import EventStatusUpdater.ExecutionDelay
import ProcessingRecoverableError._
import cats.data.EitherT
import cats.data.EitherT.leftT
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities.Project
import io.renku.graph.model.events.EventStatus.{FailureStatus, TransformationNonRecoverableFailure, TransformationRecoverableFailure, TriplesGenerated}
import io.renku.graph.model.events._
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.interpreters.TestLogger.Matcher.NotRefEqual
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.{logWorthyRecoverableErrors, nonRecoverableMalformedRepoErrors, silentRecoverableErrors}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tsprovisioning.transformation.Generators._
import tsprovisioning.transformation.TransformationStepsCreator
import tsprovisioning.triplesuploading.TriplesUploadResult._
import tsprovisioning.triplesuploading.{TransformationStepsRunner, TriplesUploadResult}

import java.time.Duration
import scala.util.{Success, Try}

class EventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  import AccessTokenFinder.Implicits._

  "process" should {

    "succeed if events are successfully turned into triples" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

      successfulTriplesTransformationAndUpload(project)

      expectEventMarkedAsDone(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logSummary(triplesGeneratedEvent, isSuccessful = true)
    }

    "mark event as TransformationRecoverableFailure and log an error " +
      s"if entity building fails with $LogWorthyRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val processingError = logWorthyRecoverableErrors.generateOne
        givenEntityBuilding(triplesGeneratedEvent, returning = leftT(processingError))

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, processingError)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logError(triplesGeneratedEvent, processingError, processingError.getMessage)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event as TransformationRecoverableFailure and refrain from logging an error " +
      s"if entity building fails with $SilentRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val processingError = silentRecoverableErrors.generateOne
        givenEntityBuilding(triplesGeneratedEvent, returning = leftT(processingError))

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, processingError)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure and log an error " +
      "if entity building fails with a non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val exception = exceptions.generateOne
        givenEntityBuilding(triplesGeneratedEvent,
                            returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, Project])
        )

        expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, exception)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logError(triplesGeneratedEvent, exception, exception.getMessage)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure and refrain from logging an error " +
      "if entity building fails with ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val exception = nonRecoverableMalformedRepoErrors.generateOne
        givenEntityBuilding(triplesGeneratedEvent,
                            returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, Project])
        )

        expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, exception)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logSummary(triplesGeneratedEvent, isSuccessful = false)
        logger.expectNoLogs(severity = Error)
      }

    "mark event with TransformationRecoverableFailure and log an error " +
      "if transforming triples fails with a LogWorthyRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

        val steps = transformationSteps[Try].generateList()
        (() => stepsCreator.createSteps)
          .expects()
          .returning(steps)

        val processingRecoverableError = logWorthyRecoverableErrors.generateOne
        val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
        (triplesUploader.run _)
          .expects(steps, project)
          .returning(failure.pure[Try].widen[TriplesUploadResult])

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, failure.error)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logError(triplesGeneratedEvent, failure.error, failure.message)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event with TransformationRecoverableFailure and refrain from log an error " +
      "if transforming triples fails with a SilentRecoverableError" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

        val steps = transformationSteps[Try].generateList()
        (() => stepsCreator.createSteps)
          .expects()
          .returning(steps)

        val processingRecoverableError = silentRecoverableErrors.generateOne
        val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
        (triplesUploader.run _)
          .expects(steps, project)
          .returning(failure.pure[Try].widen[TriplesUploadResult])

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, failure.error)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event with TransformationRecoverableFailure if transforming triples fails with SilentRecoverableError " +
      "but doesn't log any errors" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

        val steps = transformationSteps[Try].generateList()
        (() => stepsCreator.createSteps)
          .expects()
          .returning(steps)

        val failure =
          TriplesUploadResult.RecoverableFailure(SilentRecoverableError(exceptions.generateOne.getMessage))
        (triplesUploader.run _)
          .expects(steps, project)
          .returning(failure.pure[Try].widen[TriplesUploadResult])

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, failure.error)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logger.getMessages(Error).isEmpty shouldBe true
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure if transforming triples fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

      val steps = transformationSteps[Try].generateList()

      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val exception = exceptions.generateOne
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(exception.raiseError[Try, TriplesUploadResult])

      expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, exception)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logError(triplesGeneratedEvent, exception, exception.getMessage)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }

    "mark event with TransformationRecoverableFailure " +
      "if uploading triples fails with RecoverableFailure" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

        val steps = transformationSteps[Try].generateList()
        (() => stepsCreator.createSteps)
          .expects()
          .returning(steps)

        val uploadingError =
          nonEmptyStrings().map(message => RecoverableFailure(LogWorthyRecoverableError(message))).generateOne
        (triplesUploader.run _)
          .expects(steps, project)
          .returning(uploadingError.pure[Try])

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, uploadingError.error)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logError(triplesGeneratedEvent, uploadingError.error, uploadingError.message)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "mark event with TransformationNonRecoverableFailure " +
      "if uploading triples to the store fails with either NonRecoverableFailure" in new TestCase {
        val failure = NonRecoverableFailure("error")
        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val project = anyProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

        val steps = transformationSteps[Try].generateList()
        (() => stepsCreator.createSteps)
          .expects()
          .returning(steps)

        (triplesUploader.run _)
          .expects(steps, project)
          .returning(failure.pure[Try])

        expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, failure)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logError(triplesGeneratedEvent, failure, failure.message)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    "succeed and log an error if marking event as TriplesStore fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(triplesGeneratedEvent, returning = EitherT.rightT(project))

      successfulTriplesTransformationAndUpload(project)

      val exception = exceptions.generateOne
      (eventStatusUpdater
        .toTriplesStore(_: CompoundEventId, _: projects.Path, _: EventProcessingTime))
        .expects(
          triplesGeneratedEvent.compoundEventId,
          triplesGeneratedEvent.project.path,
          EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(exception.raiseError[Try, Unit])

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logError(triplesGeneratedEvent, exception, "failed to mark done in the Event Log")
      logSummary(triplesGeneratedEvent, isSuccessful = true)
    }

    "mark event as TriplesGenerated and log an error if finding an access token fails" in new TestCase {

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(exception.raiseError[Try, Option[AccessToken]])

      expectEventRolledBackToTriplesGenerated(triplesGeneratedEvent)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logger.loggedOnly(
        Error(
          message = show"$categoryName: processing failure: $triplesGeneratedEvent",
          throwableMatcher = NotRefEqual(new Exception("transformation failure -> Event rolled back", exception))
        )
      )
    }
  }

  private trait TestCase {

    val triplesGeneratedEvent = triplesGeneratedEvents.generateOne

    implicit val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne

    implicit val logger:            TestLogger[Try]        = TestLogger[Try]()
    implicit val accessTokenFinder: AccessTokenFinder[Try] = mock[AccessTokenFinder[Try]]
    val stepsCreator          = mock[TransformationStepsCreator[Try]]
    val triplesUploader       = mock[TransformationStepsRunner[Try]]
    val eventStatusUpdater    = mock[EventStatusUpdater[Try]]
    val entityBuilder         = mock[EntityBuilder[Try]]
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](maybeHistogram = None)
    val eventProcessor = new EventProcessorImpl[Try](stepsCreator,
                                                     triplesUploader,
                                                     eventStatusUpdater,
                                                     entityBuilder,
                                                     executionTimeRecorder
    )

    def givenFetchingAccessToken(forProjectPath: Path) =
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(forProjectPath, projectPathToPath)

    def successfulTriplesTransformationAndUpload(project: Project) = {
      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      (triplesUploader.run _)
        .expects(steps, project)
        .returning(Success(DeliverySuccess))
    }

    def givenEntityBuilding(event:     TriplesGeneratedEvent,
                            returning: EitherT[Try, ProcessingRecoverableError, Project]
    ) = (entityBuilder
      .buildEntity(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
      .expects(event, maybeAccessToken)
      .returning(returning)

    def expectEventMarkedAsRecoverableFailure(event: TriplesGeneratedEvent, exception: ProcessingRecoverableError) = {
      val executionDelay = exception match {
        case _: SilentRecoverableError    => ExecutionDelay(Duration.ofHours(1))
        case _: LogWorthyRecoverableError => ExecutionDelay(Duration.ofMinutes(5))
      }
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Path, _: FailureStatus, _: Throwable, _: ExecutionDelay))
        .expects(event.compoundEventId, event.project.path, TransformationRecoverableFailure, exception, executionDelay)
        .returning(().pure[Try])
    }

    def expectEventMarkedAsNonRecoverableFailure(event: TriplesGeneratedEvent, exception: Throwable) =
      (eventStatusUpdater
        .toFailure(_: CompoundEventId, _: Path, _: FailureStatus, _: Throwable))
        .expects(event.compoundEventId, event.project.path, TransformationNonRecoverableFailure, exception)
        .returning(().pure[Try])

    def expectEventMarkedAsDone(compoundEventId: CompoundEventId, projectPath: projects.Path) =
      (eventStatusUpdater
        .toTriplesStore(_: CompoundEventId, _: projects.Path, _: EventProcessingTime))
        .expects(compoundEventId,
                 projectPath,
                 EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(().pure[Try])

    def expectEventRolledBackToTriplesGenerated(event: TriplesGeneratedEvent) =
      (eventStatusUpdater
        .rollback[TriplesGenerated](_: CompoundEventId, _: projects.Path)(_: () => TriplesGenerated))
        .expects(event.compoundEventId, event.project.path, *)
        .returning(().pure[Try])

    def logSummary(triplesGeneratedEvent: TriplesGeneratedEvent, isSuccessful: Boolean): Assertion =
      logger.logged(
        Info(
          s"${commonLogMessage(triplesGeneratedEvent)} processed in ${executionTimeRecorder.elapsedTime}ms: " +
            s"${if (isSuccessful) "success" else "failure"}"
        )
      )

    def logError(event: TriplesGeneratedEvent, exception: Throwable, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(event)} $message", NotRefEqual(exception)))

    def commonLogMessage(event: TriplesGeneratedEvent): String =
      s"$categoryName: ${event.compoundEventId}, projectPath = ${event.project.path}"
  }
}