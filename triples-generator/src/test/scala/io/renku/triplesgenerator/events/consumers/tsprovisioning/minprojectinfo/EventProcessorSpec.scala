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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package minprojectinfo

import CategoryGenerators._
import ProcessingRecoverableError._
import cats.data.EitherT
import cats.data.EitherT.{leftT, right, rightT}
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{entities, projects}
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
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesgenerator.generators.ErrorGenerators.{logWorthyRecoverableErrors, nonRecoverableMalformedRepoErrors, silentRecoverableErrors}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import transformation.Generators._
import transformation.TransformationStepsCreator
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}
import triplesuploading.TriplesUploadResult._

class EventProcessorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  import AccessTokenFinder.Implicits._

  "process" should {

    "succeed and send ProjectActivated event " +
      "if events are successfully turned into triples" in new TestCase {

        givenFetchingAccessToken(forProjectPath = event.project.path)
          .returning(maybeAccessToken.pure[IO])

        val project = anyNonRenkuProjectEntities.generateOne.to[entities.Project]
        givenEntityBuilding(event, returning = rightT(project))

        givenProjectExistenceCheck(project, returning = false.pure[IO])

        successfulTriplesTransformationAndUpload(project)

        givenProjectActivatedEventSent(event.project.path, returning = ().pure[IO])

        eventProcessor.process(event).unsafeRunSync() shouldBe ()

        logger.logged(Info(s"${commonLogMessage(event)} accepted"))
        logSummary(event, message = "success")
      }

    "do nothing if the project already exists in the TS" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyNonRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = true.pure[IO])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logger.logged(Info(s"${commonLogMessage(event)} accepted"))
      logSummary(event, message = "skipped")
    }

    "log an error if entity building fails with LogWorthyRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val processingError = logWorthyRecoverableErrors.generateOne
      givenEntityBuilding(event, returning = leftT(processingError))

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, processingError, processingError.getMessage)
      logSummary(event, message = "failure")
    }

    "log an error if entity building fails with a non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val exception = exceptions.generateOne
      givenEntityBuilding(event, returning = right[ProcessingRecoverableError](exception.raiseError[IO, Project]))

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, exception, exception.getMessage)
      logSummary(event, message = "failure")
    }

    "do not log an error if entity building fails with SilentRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val processingError = silentRecoverableErrors.generateOne
      givenEntityBuilding(event, returning = leftT(processingError))

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logSummary(event, message = "failure")
    }

    "do not log an error if entity building fails with ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val exception = nonRecoverableMalformedRepoErrors.generateOne
      givenEntityBuilding(event, returning = right[ProcessingRecoverableError](exception.raiseError[IO, Project]))

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logSummary(event, message = "failure")
      logger.expectNoLogs(severity = Error)
    }

    "log an error if transforming triples fails with a LogWorthyRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      val processingRecoverableError = logWorthyRecoverableErrors.generateOne
      val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
      successfulStepsCreation(project, failure.pure[IO])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, failure.error, failure.message)
      logSummary(event, message = "failure")
    }

    "do not log an error if transforming triples fails with a SilentRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      val processingRecoverableError = silentRecoverableErrors.generateOne
      val failure                    = TriplesUploadResult.RecoverableFailure(processingRecoverableError)
      successfulStepsCreation(project, failure.pure[IO])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logSummary(event, message = "failure")
    }

    "do not log an error if transforming triples fails with SilentRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      val failure =
        TriplesUploadResult.RecoverableFailure(SilentRecoverableError(exceptions.generateOne.getMessage))
      successfulStepsCreation(project, failure.pure[IO])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logger.getMessages(Error).isEmpty shouldBe true
      logSummary(event, message = "failure")
    }

    "log an error if transforming triples fails with an unknown Exception" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      val exception = exceptions.generateOne
      successfulStepsCreation(project, exception.raiseError[IO, TriplesUploadResult])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, exception, exception.getMessage)
      logSummary(event, message = "failure")
    }

    "log an error if uploading triples fails with RecoverableFailure" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      val failure =
        nonEmptyStrings().map(message => RecoverableFailure(LogWorthyRecoverableError(message))).generateOne
      successfulStepsCreation(project, failure.pure[IO])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, failure.error, failure.message)
      logSummary(event, message = "failure")
    }

    "log an error if uploading triples to the store fails with a NonRecoverableFailure" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      val failure = NonRecoverableFailure("error")
      successfulStepsCreation(project, failure.pure[IO])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logError(event, failure, failure.message)
      logSummary(event, message = "failure")
    }

    "log an error if finding project access token fails" in new TestCase {

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logger.logged(Error(message = show"$categoryName: $event processing failure", exception))
    }

    "log an error when sending ProjectActivated event fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = event.project.path)
        .returning(maybeAccessToken.pure[IO])

      val project = anyNonRenkuProjectEntities.generateOne.to[entities.Project]
      givenEntityBuilding(event, returning = rightT(project))

      givenProjectExistenceCheck(project, returning = false.pure[IO])

      successfulTriplesTransformationAndUpload(project)

      val exception = exceptions.generateOne
      givenProjectActivatedEventSent(event.project.path, returning = exception.raiseError[IO, Unit])

      eventProcessor.process(event).unsafeRunSync() shouldBe ()

      logger.logged(
        Info(s"${commonLogMessage(event)} accepted"),
        Error(s"${commonLogMessage(event)} sending ${ProjectActivated.categoryName} event failed", exception)
      )
      logSummary(event, message = "success")
    }
  }

  private trait TestCase {

    val event = minProjectInfoEvents.generateOne

    implicit val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne

    implicit val logger:            TestLogger[IO]        = TestLogger[IO]()
    implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
    private val stepsCreator            = mock[TransformationStepsCreator[IO]]
    private val stepsRunner             = mock[TransformationStepsRunner[IO]]
    private val projectExistenceChecker = mock[ProjectExistenceChecker[IO]]
    private val entityBuilder           = mock[EntityBuilder[IO]]
    private val tgClient                = mock[triplesgenerator.api.events.Client[IO]]
    private val executionTimeRecorder   = TestExecutionTimeRecorder[IO](maybeHistogram = None)
    val eventProcessor = new EventProcessorImpl[IO](stepsCreator,
                                                    stepsRunner,
                                                    entityBuilder,
                                                    projectExistenceChecker,
                                                    tgClient,
                                                    executionTimeRecorder
    )

    def givenFetchingAccessToken(forProjectPath: Path) =
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(forProjectPath, projectPathToPath)

    def successfulTriplesTransformationAndUpload(project: Project) =
      successfulStepsCreation(project, runningToReturn = DeliverySuccess.pure[IO])

    def successfulStepsCreation(project: Project, runningToReturn: IO[TriplesUploadResult]) = {
      val steps = transformationSteps[IO].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      givenStepsRunnerFor(steps, project, returning = runningToReturn)
    }

    private def givenStepsRunnerFor(steps:     List[TransformationStep[IO]],
                                    project:   Project,
                                    returning: IO[TriplesUploadResult]
    ) = (stepsRunner
      .run(_: List[TransformationStep[IO]], _: Project))
      .expects(steps, project)
      .returning(returning)

    def givenProjectExistenceCheck(project: Project, returning: IO[Boolean]) =
      (projectExistenceChecker.checkProjectExists _)
        .expects(project.resourceId)
        .returning(returning)

    def givenEntityBuilding(event: MinProjectInfoEvent, returning: EitherT[IO, ProcessingRecoverableError, Project]) =
      (entityBuilder
        .buildEntity(_: MinProjectInfoEvent)(_: Option[AccessToken]))
        .expects(event, maybeAccessToken)
        .returning(returning)

    def givenProjectActivatedEventSent(path: projects.Path, returning: IO[Unit]) =
      (tgClient
        .send(_: ProjectActivated))
        .expects(where((ev: ProjectActivated) => ev.path == path))
        .returning(returning)

    def logSummary(event: MinProjectInfoEvent, message: String): Assertion = logger.logged(
      Info(s"${commonLogMessage(event)} processed in ${executionTimeRecorder.elapsedTime}ms: $message")
    )

    def logError(event: MinProjectInfoEvent, exception: Throwable, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(event)} $message", NotRefEqual(exception)))

    def commonLogMessage(event: MinProjectInfoEvent): String = show"$categoryName: $event"
  }
}
