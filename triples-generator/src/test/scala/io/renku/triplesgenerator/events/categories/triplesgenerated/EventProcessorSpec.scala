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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.MonadError
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import io.prometheus.client.Histogram
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities.Project
import io.renku.graph.model.events.EventStatus.{TransformationNonRecoverableFailure, TransformationRecoverableFailure, TriplesGenerated}
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
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.EventStatusUpdater
import io.renku.triplesgenerator.events.categories.triplesgenerated.EventProcessor.eventsProcessingTimesBuilder
import io.renku.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.Generators._
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TransformationStepsCreator
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult._
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.{TransformationStepsRunner, TriplesUploadResult}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

class EventProcessorSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  import AccessTokenFinder._

  "process" should {

    "succeed if events are successfully turned into triples" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(project))

      successfulTriplesTransformationAndUpload(project)

      expectEventMarkedAsDone(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logSummary(triplesGeneratedEvent, isSuccessful = true)

      verifyMetricsCollected()
    }

    s"mark event with TransformationRecoverableFailure if deserialization fails with ProcessingRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception       = exceptions.generateOne
      val processingError = new Exception(exception) with ProcessingRecoverableError
      givenDeserialization(triplesGeneratedEvent, returning = EitherT.leftT(processingError))

      expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, processingError)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logError(triplesGeneratedEvent, processingError, processingError.getMessage)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }
    "mark event with TransformationNonRecoverableFailure if deserialization fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val exception = exceptions.generateOne

      givenDeserialization(triplesGeneratedEvent,
                           returning = EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, Project])
      )

      expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, exception)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logError(triplesGeneratedEvent, exception, exception.getMessage)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }

    s"mark event with TransformationRecoverableFailure if transforming triples fails with $TransformationRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(project))

      val steps = transformationSteps[Try].generateList()
      (() => stepsCreator.createSteps)
        .expects()
        .returning(steps)

      val failure = TriplesUploadResult.RecoverableFailure(exceptions.generateOne.getMessage)
      (triplesUploader.run _)
        .expects(steps, project)
        .returning(failure.pure[Try].widen[TriplesUploadResult])

      expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, failure)

      eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

      logError(triplesGeneratedEvent, failure, failure.message)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }

    "mark event with TransformationNonRecoverableFailure if transforming triples fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(project))

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
      s"if uploading triples fails with $RecoverableFailure" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(maybeAccessToken.pure[Try])

        val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
        givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(project))

        val steps = transformationSteps[Try].generateList()
        (() => stepsCreator.createSteps)
          .expects()
          .returning(steps)

        val uploadingError = nonEmptyStrings().map(RecoverableFailure.apply).generateOne
        (triplesUploader.run _)
          .expects(steps, project)
          .returning(uploadingError.pure[Try])

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, uploadingError)

        eventProcessor.process(triplesGeneratedEvent) shouldBe ().pure[Try]

        logError(triplesGeneratedEvent, uploadingError, uploadingError.message)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    " mark event with TransformationNonRecoverableFailure " +
      s"if uploading triples to the store fails with either $InvalidTriplesFailure or $InvalidUpdatesFailure" in new TestCase {

        (InvalidTriplesFailure("error") +: InvalidUpdatesFailure("error") +: Nil) foreach { failure =>
          givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
            .returning(maybeAccessToken.pure[Try])

          val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
          givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(project))

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
          logger.reset()
        }
      }

    "succeed and log an error if marking event as TriplesStore fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(maybeAccessToken.pure[Try])

      val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(project))

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
          message =
            s"$categoryName: Triples Generated Event processing failure: ${triplesGeneratedEvent.compoundEventId}, projectPath: ${triplesGeneratedEvent.project.path}",
          throwableMatcher = NotRefEqual(new Exception("transformation failure -> Event rolled back", exception))
        )
      )
    }
  }

  "eventsProcessingTimes histogram" should {

    "have 'triples_transformation_processing_times' name" in {
      eventsProcessingTimes.startTimer().observeDuration()

      eventsProcessingTimes.collect().asScala.headOption.map(_.name) shouldBe Some(
        "triples_transformation_processing_times"
      )
    }

    "be registered in the Metrics Registry" in {

      val metricsRegistry = mock[MetricsRegistry]

      (metricsRegistry
        .register[IO, Histogram, Histogram.Builder](_: Histogram.Builder)(_: MonadError[IO, Throwable]))
        .expects(eventsProcessingTimesBuilder, *)
        .returning(eventsProcessingTimes.pure[IO])

      implicit val logger: TestLogger[IO] = TestLogger[IO]()
      EventProcessor[IO](
        metricsRegistry,
        Throttler.noThrottling[IO],
        new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
      ).unsafeRunSync()
    }
  }

  private lazy val eventsProcessingTimes = eventsProcessingTimesBuilder.create()

  private trait TestCase {
    val triplesGeneratedEvent = triplesGeneratedEvents.generateOne

    implicit val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne

    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val stepsCreator          = mock[TransformationStepsCreator[Try]]
    val triplesUploader       = mock[TransformationStepsRunner[Try]]
    val eventStatusUpdater    = mock[EventStatusUpdater[Try]]
    val jsonLDDeserializer    = mock[JsonLDDeserializer[Try]]
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](Option(eventsProcessingTimes))
    val eventProcessor = new EventProcessorImpl[Try](
      accessTokenFinder,
      stepsCreator,
      triplesUploader,
      eventStatusUpdater,
      jsonLDDeserializer,
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

    def givenDeserialization(event:     TriplesGeneratedEvent,
                             returning: EitherT[Try, ProcessingRecoverableError, Project]
    ) = (jsonLDDeserializer
      .deserializeToModel(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
      .expects(event, maybeAccessToken)
      .returning(returning)

    def expectEventMarkedAsRecoverableFailure(event: TriplesGeneratedEvent, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(event.compoundEventId, event.project.path, TransformationRecoverableFailure, exception)
        .returning(().pure[Try])

    def expectEventMarkedAsNonRecoverableFailure(event: TriplesGeneratedEvent, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
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
            s"${if (isSuccessful) "was successfully uploaded" else "failed to upload"}"
        )
      )

    def logError(event: TriplesGeneratedEvent, exception: Exception, message: String = "failed"): Assertion =
      logger.logged(Error(s"${commonLogMessage(event)} $message", NotRefEqual(exception)))

    def commonLogMessage(event: TriplesGeneratedEvent): String =
      s"$categoryName: ${event.compoundEventId}, projectPath: ${event.project.path}"

    def verifyMetricsCollected() =
      eventsProcessingTimes
        .collect()
        .asScala
        .flatMap(_.samples.asScala.map(_.name))
        .exists(_ startsWith "triples_transformation_processing_times") shouldBe true
  }

}
