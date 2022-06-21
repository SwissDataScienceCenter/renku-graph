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
package minprojectinfo

import CategoryGenerators._
import ProcessingRecoverableError._
import cats.data.EitherT
import cats.data.EitherT.{leftT, rightT}
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities
import io.renku.graph.model.entities.Project
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities._
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
import transformation.Generators._
import transformation.TransformationStepsCreator
import triplesuploading.TriplesUploadResult._
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}

import scala.util.{Success, Try}

class EventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  import AccessTokenFinder.Implicits._

  "process" should {

    "succeed if events are successfully turned into triples" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyNonRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      successfulTriplesTransformationAndUpload(project)

      eventProcessor.process(event) shouldBe ().pure[Try]

      logSummary(event, isSuccessful = true)
    }

    "log an error if entity building fails with LogWorthyRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val processingError = logWorthyRecoverableErrors.generateOne
      givenEntityBuilding(event, returning = leftT(processingError))

      eventProcessor.process(event) shouldBe ().pure[Try]

      logError(event, processingError, processingError.getMessage)
      logSummary(event, isSuccessful = false)
    }

    "log an error if entity building fails with a non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = exceptions.generateOne
      givenEntityBuilding(event,
                          returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, Project])
      )

      eventProcessor.process(event) shouldBe ().pure[Try]

      logError(event, exception, exception.getMessage)
      logSummary(event, isSuccessful = false)
    }

    s"do not log an error if entity building fails with SilentRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val processingError = silentRecoverableErrors.generateOne
      givenEntityBuilding(event, returning = leftT(processingError))

      eventProcessor.process(event) shouldBe ().pure[Try]

      logSummary(event, isSuccessful = false)
    }

    "do not log an error if entity building fails with ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = nonRecoverableMalformedRepoErrors.generateOne
      givenEntityBuilding(event,
                          returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, Project])
      )

      eventProcessor.process(event) shouldBe ().pure[Try]

      logSummary(event, isSuccessful = false)
      logger.expectNoLogs(severity = Error)
    }

    "log an error if transforming triples fails with a LogWorthyRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val processingRecoverableError = logWorthyRecoverableErrors.generateOne
      val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(failure.pure[Try].widen[TriplesUploadResult])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logError(event, failure.error, failure.message)
      logSummary(event, isSuccessful = false)
    }

    "do not log an error if transforming triples fails with a SilentRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val processingRecoverableError = silentRecoverableErrors.generateOne
      val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(failure.pure[Try].widen[TriplesUploadResult])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logSummary(event, isSuccessful = false)
    }

    "do not log an error if transforming triples fails with SilentRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val failure =
        TriplesUploadResult.RecoverableFailure(SilentRecoverableError(exceptions.generateOne.getMessage))
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(failure.pure[Try].widen[TriplesUploadResult])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logger.getMessages(Error).isEmpty shouldBe true
      logSummary(event, isSuccessful = false)
    }

    "log an error if transforming triples fails with an unknown Exception" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      val steps = transformationSteps[Try].generateList()

      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val exception = exceptions.generateOne
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(exception.raiseError[Try, TriplesUploadResult])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logError(event, exception, exception.getMessage)
      logSummary(event, isSuccessful = false)
    }

    "log an error if uploading triples fails with RecoverableFailure" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val uploadingError =
        nonEmptyStrings().map(message => RecoverableFailure(LogWorthyRecoverableError(message))).generateOne
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(uploadingError.pure[Try])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logError(event, uploadingError.error, uploadingError.message)
      logSummary(event, isSuccessful = false)
    }

    "log an error if uploading triples to the store fails with a NonRecoverableFailure" in new TestCase {
      val failure = NonRecoverableFailure("error")
      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      (triplesUploader.run _)
        .expects(steps, project)
        .returning(failure.pure[Try])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logError(event, failure, failure.message)
      logSummary(event, isSuccessful = false)
    }

    "log an error if finding an access token fails" in new TestCase {

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(exception.raiseError[Try, Option[AccessToken]])

      eventProcessor.process(event) shouldBe ().pure[Try]

      logger.loggedOnly(Error(message = show"$categoryName: processing failure: $event", exception))
    }
  }

  private trait TestCase {

    val event = minProjectInfoEvents.generateOne

    implicit val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne

    implicit val logger:            TestLogger[Try]        = TestLogger[Try]()
    implicit val accessTokenFinder: AccessTokenFinder[Try] = mock[AccessTokenFinder[Try]]
    val stepsCreator          = mock[TransformationStepsCreator[Try]]
    val triplesUploader       = mock[TransformationStepsRunner[Try]]
    val entityBuilder         = mock[EntityBuilder[Try]]
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](maybeHistogram = None)
    val eventProcessor =
      new EventProcessorImpl[Try](stepsCreator, triplesUploader, entityBuilder, executionTimeRecorder)

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

    def givenEntityBuilding(event: MinProjectInfoEvent, returning: EitherT[Try, ProcessingRecoverableError, Project]) =
      (entityBuilder
        .buildEntity(_: MinProjectInfoEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(returning)

    def logSummary(event: MinProjectInfoEvent, isSuccessful: Boolean): Assertion = logger.logged(
      Info(
        s"${commonLogMessage(event)} processed in ${executionTimeRecorder.elapsedTime}ms: " +
          s"${if (isSuccessful) "success" else "failure"}"
      )
    )

    def logError(event: MinProjectInfoEvent, exception: Throwable, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(event)} $message", NotRefEqual(exception)))

    def commonLogMessage(event: MinProjectInfoEvent): String = show"$categoryName: $event"
  }
}
