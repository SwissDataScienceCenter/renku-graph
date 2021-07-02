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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.MonadError
import cats.data.EitherT
import cats.data.EitherT.{leftT, rightT}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.graph.model.events.EventStatus.{TransformationNonRecoverableFailure, TransformationRecoverableFailure, TriplesGenerated}
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.interpreters.TestLogger.Matcher.NotRefEqual
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.IOTriplesGeneratedEventProcessor.eventsProcessingTimesBuilder
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators.curatedTriplesObjects
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.{CuratedTriples, TriplesTransformer}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.Uploader
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
import scala.util.{Success, Try}

class TriplesGeneratedEventProcessorSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  import AccessTokenFinder._

  "process" should {

    "succeed if events are successfully turned into triples" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.pure(maybeAccessToken))

      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(projectMetadatas.generateOne))

      successfulTriplesCurationAndUpload(triplesGeneratedEvent)

      expectEventMarkedAsDone(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

      logSummary(triplesGeneratedEvent, isSuccessful = true)

      verifyMetricsCollected()
    }

    s"mark event with TransformationRecoverableFailure if deserialization fails with ProcessingRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.pure(maybeAccessToken))

      val exception       = exceptions.generateOne
      val processingError = new Exception(exception) with ProcessingRecoverableError
      givenDeserialization(triplesGeneratedEvent, returning = EitherT.leftT(processingError))

      expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, processingError)

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

      logError(triplesGeneratedEvent, processingError, processingError.getMessage)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }
    "mark event with TransformationNonRecoverableFailure if deserialization fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.pure(maybeAccessToken))

      val exception = exceptions.generateOne

      givenDeserialization(triplesGeneratedEvent,
                           returning =
                             EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, ProjectMetadata])
      )

      expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, exception)

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

      logError(triplesGeneratedEvent, exception, exception.getMessage)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }

    s"mark event with TransformationRecoverableFailure if transforming triples fails with $CurationRecoverableError" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.pure(maybeAccessToken))

      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(projectMetadatas.generateOne))

      val exeption          = exceptions.generateOne
      val curationException = CurationRecoverableError(nonBlankStrings().generateOne.value, exeption)
      (triplesTransformer
        .transform(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, maybeAccessToken)
        .returning(leftT[Try, CuratedTriples[Try]](curationException))

      expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, curationException)

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

      logError(triplesGeneratedEvent, curationException, curationException.message)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }

    "mark event with TransformationNonRecoverableFailure if transforming triples fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.pure(maybeAccessToken))

      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(projectMetadatas.generateOne))

      val exception = exceptions.generateOne
      (triplesTransformer
        .transform(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, maybeAccessToken)
        .returning(EitherT.liftF[Try, ProcessingRecoverableError, CuratedTriples[Try]](context.raiseError(exception)))

      expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, exception)

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

      logError(triplesGeneratedEvent, exception, exception.getMessage)
      logSummary(triplesGeneratedEvent, isSuccessful = false)
    }

    "mark event with TransformationRecoverableFailure " +
      s"if uploading triples to the dataset fails with $RecoverableFailure" in new TestCase {

        givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
          .returning(context.pure(maybeAccessToken))

        givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(projectMetadatas.generateOne))

        val curatedTriples = curatedTriplesObjects[Try].generateOne
        (triplesTransformer
          .transform(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
          .expects(triplesGeneratedEvent, maybeAccessToken)
          .returning(rightT[Try, ProcessingRecoverableError](curatedTriples))

        val uploadingError = nonEmptyStrings().map(RecoverableFailure.apply).generateOne
        (triplesUploader
          .upload(_: CuratedTriples[Try]))
          .expects(curatedTriples)
          .returning(context.pure(uploadingError))

        expectEventMarkedAsRecoverableFailure(triplesGeneratedEvent, uploadingError)

        eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

        logError(triplesGeneratedEvent, uploadingError, uploadingError.message)
        logSummary(triplesGeneratedEvent, isSuccessful = false)
      }

    " mark event with TransformationNonRecoverableFailure " +
      s"if uploading triples to the store fails with either $InvalidTriplesFailure or $InvalidUpdatesFailure" in new TestCase {

        (InvalidTriplesFailure("error") +: InvalidUpdatesFailure("error") +: Nil) foreach { failure =>
          givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
            .returning(context.pure(maybeAccessToken))

          givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(projectMetadatas.generateOne))

          val curatedTriples = curatedTriplesObjects[Try].generateOne
          (triplesTransformer
            .transform(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
            .expects(triplesGeneratedEvent, maybeAccessToken)
            .returning(rightT[Try, ProcessingRecoverableError](curatedTriples))

          (triplesUploader
            .upload(_: CuratedTriples[Try]))
            .expects(curatedTriples)
            .returning(context.pure(failure))

          expectEventMarkedAsNonRecoverableFailure(triplesGeneratedEvent, failure)

          eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

          logError(triplesGeneratedEvent, failure, failure.message)
          logSummary(triplesGeneratedEvent, isSuccessful = false)
          logger.reset()
        }
      }

    "succeed and log an error if marking event as TriplesStore fails" in new TestCase {

      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.pure(maybeAccessToken))

      givenDeserialization(triplesGeneratedEvent, returning = EitherT.rightT(projectMetadatas.generateOne))

      successfulTriplesCurationAndUpload(triplesGeneratedEvent)

      val exception = exceptions.generateOne
      (eventStatusUpdater
        .toTriplesStore(_: CompoundEventId, _: projects.Path, _: EventProcessingTime))
        .expects(
          triplesGeneratedEvent.compoundEventId,
          triplesGeneratedEvent.project.path,
          EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(context.raiseError(exception))

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

      logError(triplesGeneratedEvent, exception, "failed to mark done in the Event Log")
      logSummary(triplesGeneratedEvent, isSuccessful = true)
    }

    "mark event as TriplesGenerated and log an error if finding an access token fails" in new TestCase {

      val exception = exceptions.generateOne
      givenFetchingAccessToken(forProjectPath = triplesGeneratedEvent.project.path)
        .returning(context.raiseError(exception))

      expectEventRolledBackToTriplesGenerated(triplesGeneratedEvent)

      eventProcessor.process(triplesGeneratedEvent) shouldBe context.unit

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

      val metricsRegistry = mock[MetricsRegistry[IO]]

      (metricsRegistry
        .register[Histogram, Histogram.Builder](_: Histogram.Builder)(_: MonadError[IO, Throwable]))
        .expects(eventsProcessingTimesBuilder, *)
        .returning(IO.pure(eventsProcessingTimes))

      val logger = TestLogger[IO]()
      IOTriplesGeneratedEventProcessor(
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
    val context               = MonadError[Try, Throwable]
    val triplesGeneratedEvent = triplesGeneratedEvents.generateOne

    val maybeAccessToken = Gen.option(accessTokens).generateOne

    val accessTokenFinder     = mock[AccessTokenFinder[Try]]
    val triplesTransformer    = mock[TriplesTransformer[Try]]
    val triplesUploader       = mock[Uploader[Try]]
    val eventStatusUpdater    = mock[EventStatusUpdater[Try]]
    val jsonLDDeserializer    = mock[JsonLDDeserializer[Try]]
    val logger                = TestLogger[Try]()
    val executionTimeRecorder = TestExecutionTimeRecorder[Try](logger, Option(eventsProcessingTimes))
    val eventProcessor = new TriplesGeneratedEventProcessor[Try](
      accessTokenFinder,
      triplesTransformer,
      triplesUploader,
      eventStatusUpdater,
      jsonLDDeserializer,
      logger,
      executionTimeRecorder
    )

    def givenFetchingAccessToken(forProjectPath: Path) =
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(forProjectPath, projectPathToPath)

    def successfulTriplesCurationAndUpload(triplesGeneratedEvent: TriplesGeneratedEvent) = {
      val curatedTriples = CuratedTriples[Try](JsonLDTriples(triplesGeneratedEvent.triples.toJson), Nil)
      (triplesTransformer
        .transform(_: TriplesGeneratedEvent)(_: Option[AccessToken]))
        .expects(triplesGeneratedEvent, maybeAccessToken)
        .returning(rightT[Try, ProcessingRecoverableError](curatedTriples))

      (triplesUploader
        .upload(_: CuratedTriples[Try]))
        .expects(curatedTriples)
        .returning(Success(DeliverySuccess))
    }

    def givenDeserialization(event:     TriplesGeneratedEvent,
                             returning: EitherT[Try, ProcessingRecoverableError, ProjectMetadata]
    ) =
      (jsonLDDeserializer.deserializeToModel _).expects(event).returning(returning)

    def expectEventMarkedAsRecoverableFailure(event: TriplesGeneratedEvent, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(event.compoundEventId, event.project.path, TransformationRecoverableFailure, exception)
        .returning(context.unit)

    def expectEventMarkedAsNonRecoverableFailure(event: TriplesGeneratedEvent, exception: Throwable) =
      (eventStatusUpdater.toFailure _)
        .expects(event.compoundEventId, event.project.path, TransformationNonRecoverableFailure, exception)
        .returning(context.unit)

    def expectEventMarkedAsDone(compoundEventId: CompoundEventId, projectPath: projects.Path) =
      (eventStatusUpdater
        .toTriplesStore(_: CompoundEventId, _: projects.Path, _: EventProcessingTime))
        .expects(compoundEventId,
                 projectPath,
                 EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(context.unit)

    def expectEventMarkedAsTriplesGenerated(event: TriplesGeneratedEvent) =
      (eventStatusUpdater
        .toTriplesGenerated(_: CompoundEventId,
                            _: projects.Path,
                            _: JsonLDTriples,
                            _: SchemaVersion,
                            _: EventProcessingTime
        ))
        .expects(
          event.compoundEventId,
          event.project.path,
          JsonLDTriples(event.triples.toJson),
          event.schemaVersion,
          EventProcessingTime(Duration.ofMillis(executionTimeRecorder.elapsedTime.value))
        )
        .returning(context.unit)

    def expectEventRolledBackToTriplesGenerated(event: TriplesGeneratedEvent) =
      (eventStatusUpdater
        .rollback[TriplesGenerated](_: CompoundEventId, _: projects.Path)(_: () => TriplesGenerated))
        .expects(event.compoundEventId, event.project.path, *)
        .returning(context.unit)

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
