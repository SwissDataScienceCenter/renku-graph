package ch.datascience.webhookservice.queue

import java.nio.file.Path

import akka.event.LoggingAdapter
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.QueueOfferResult.Enqueued
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl, PushEvent}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}

import scala.concurrent.{ExecutionContext, Future}

class PushEventQueueSpec
  extends WordSpec
    with MockFactory
    with ScalatestRouteTest
    with Eventually
    with ScalaFutures
    with IntegrationPatience {

  "offer" should {

    "return Enqueued and trigger triples generation and upload to Fuseki" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val triplesFile: TriplesFile = triplesFiles.generateOne

      givenGenerateTriples(pushEvent) returning Future.successful(Right(triplesFile))
      givenTriplesFileUpload(triplesFile) returning Future.successful()

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload(triplesFile)
        verifyTriplesFileRemoval(triplesFile)
      }
    }

    "return Enqueued and log an error " +
      "when generating the triples returns an error" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val exception = new Exception("error")

      givenGenerateTriples(pushEvent) returning Future.successful(Left(exception))

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyGeneratingTriplesError(pushEvent, exception)

        (fusekiConnector.uploadFile(_: TriplesFile)(_: ExecutionContext))
          .verify(*, *)
          .never()

        (fileCommands.removeSilently(_: Path))
          .verify(*)
          .never()
      }
    }

    "return Enqueued and log an error " +
      "when uploading the triples returns an error" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val triplesFile: TriplesFile = triplesFiles.generateOne

      givenGenerateTriples(pushEvent) returning Future.successful(Right(triplesFile))

      val exception = new Exception("error")
      givenTriplesFileUpload(triplesFile) returning Future.failed(exception)

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload(triplesFile)
        verifyUploadingTriplesFileError(pushEvent, exception)
        verifyTriplesFileRemoval(triplesFile)
      }
    }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val pushEvent1: PushEvent = pushEvents.generateOne
      val triplesFile1: TriplesFile = triplesFiles.generateOne
      givenGenerateTriples(pushEvent1) returning Future.successful(Right(triplesFile1))
      givenTriplesFileUpload(triplesFile1) returning Future.successful()

      val pushEvent2: PushEvent = pushEvents.generateOne
      val exception2 = new Exception("error 2")
      givenGenerateTriples(pushEvent2) returning Future.successful(Left(exception2))

      val pushEvent3: PushEvent = pushEvents.generateOne
      val triplesFile3: TriplesFile = triplesFiles.generateOne
      val exception3 = new Exception("error 3")
      givenGenerateTriples(pushEvent3) returning Future.successful(Right(triplesFile3))
      givenTriplesFileUpload(triplesFile3) returning Future.failed(exception3)

      val pushEvent4: PushEvent = pushEvents.generateOne
      val triplesFile4: TriplesFile = triplesFiles.generateOne
      givenGenerateTriples(pushEvent4) returning Future.successful(Right(triplesFile4))
      givenTriplesFileUpload(triplesFile4) returning Future.successful()

      pushEventQueue.offer(pushEvent1).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent2).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent3).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent4).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload(triplesFile1)

        verifyGeneratingTriplesError(pushEvent2, exception2)

        verifyUploadingTriplesFileError(pushEvent3, exception3)

        verifyTriplesFileUpload(triplesFile4)

        verifyTriplesFileRemoval(triplesFile1)
        verifyTriplesFileRemoval(triplesFile3)
        verifyTriplesFileRemoval(triplesFile4)
      }
    }
  }

  private trait TestCase {
    val triplesFinder: TriplesFinder = mock[TriplesFinder]
    val fusekiConnector: FusekiConnector = stub[FusekiConnector]
    val logger: LoggingAdapter = stub[LoggingAdapter]
    val fileCommands: Commands.File = stub[Commands.File]
    val pushEventQueue = new PushEventQueue(
      triplesFinder,
      fusekiConnector,
      QueueConfig(BufferSize(1), TriplesFinderThreads(1), FusekiUploadThreads(1)),
      fileCommands,
      logger
    )

    def givenGenerateTriples(event: PushEvent) = new {
      def returning(outcome: Future[Either[Exception, TriplesFile]]) =
        (triplesFinder.generateTriples(_: GitRepositoryUrl, _: CheckoutSha)(_: ExecutionContext))
          .expects(event.gitRepositoryUrl, event.checkoutSha, implicitly[ExecutionContext])
          .returning(outcome)
    }

    def givenTriplesFileUpload(triplesFile: TriplesFile) = new {
      def returning(outcome: Future[Unit]) =
        (fusekiConnector.uploadFile(_: TriplesFile)(_: ExecutionContext))
          .when(triplesFile, implicitly[ExecutionContext])
          .returning(outcome)
    }

    def verifyGeneratingTriplesError(pushEvent: PushEvent, exception: Exception) =
      (logger.error(_: String))
        .verify(s"Generating triples for $pushEvent failed: ${exception.getMessage}")

    def verifyUploadingTriplesFileError(pushEvent: PushEvent, exception: Exception) =
      (logger.error(_: String))
        .verify(s"Uploading triples for $pushEvent failed: ${exception.getMessage}")

    def verifyTriplesFileUpload(triplesFile: TriplesFile) =
      (fusekiConnector.uploadFile(_: TriplesFile)(_: ExecutionContext))
        .verify(triplesFile, implicitly[ExecutionContext])

    def verifyTriplesFileRemoval(triplesFile: TriplesFile) =
      (fileCommands.removeSilently(_: Path))
        .verify(triplesFile.value)
  }
}
