package ch.datascience.webhookservice.queue

import akka.event.LoggingAdapter
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.QueueOfferResult.Enqueued
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl, ProjectName, PushEvent}
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

    "return Enqueued and generating the triples and uploading them to Fuseki is triggered" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val triplesFile: TriplesFile = triplesFiles.generateOne

      givenGenerateTriples(pushEvent) returning Future.successful(Right(triplesFile))
      givenTriplesFileUpload(triplesFile, pushEvent.projectName) returning Future.successful()

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload(triplesFile, pushEvent.projectName)
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

        (fusekiConnector.uploadFile(_: TriplesFile, _: ProjectName)(_: ExecutionContext))
          .verify(*, *, *)
          .never()
      }
    }

    "return Enqueued and log an error " +
      "when uploading the triples returns an error" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val triplesFile: TriplesFile = triplesFiles.generateOne

      givenGenerateTriples(pushEvent) returning Future.successful(Right(triplesFile))

      val exception = new Exception("error")
      givenTriplesFileUpload(triplesFile, pushEvent.projectName) returning Future.failed(exception)

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload(triplesFile, pushEvent.projectName)
        verifyUploadingTriplesFileError(pushEvent, exception)
      }
    }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val pushEvent1: PushEvent = pushEvents.generateOne
      val triplesFile1: TriplesFile = triplesFiles.generateOne
      givenGenerateTriples(pushEvent1) returning Future.successful(Right(triplesFile1))
      givenTriplesFileUpload(triplesFile1, pushEvent1.projectName) returning Future.successful()

      val pushEvent2: PushEvent = pushEvents.generateOne
      val exception2 = new Exception("error 2")
      givenGenerateTriples(pushEvent2) returning Future.successful(Left(exception2))

      val pushEvent3: PushEvent = pushEvents.generateOne
      val triplesFile3: TriplesFile = triplesFiles.generateOne
      val exception3 = new Exception("error 3")
      givenGenerateTriples(pushEvent3) returning Future.successful(Right(triplesFile3))
      givenTriplesFileUpload(triplesFile3, pushEvent3.projectName) returning Future.failed(exception3)

      val pushEvent4: PushEvent = pushEvents.generateOne
      val triplesFile4: TriplesFile = triplesFiles.generateOne
      givenGenerateTriples(pushEvent4) returning Future.successful(Right(triplesFile4))
      givenTriplesFileUpload(triplesFile4, pushEvent4.projectName) returning Future.successful()

      pushEventQueue.offer(pushEvent1).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent2).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent3).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent4).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload(triplesFile1, pushEvent1.projectName)

        verifyGeneratingTriplesError(pushEvent2, exception2)

        verifyUploadingTriplesFileError(pushEvent3, exception3)

        verifyTriplesFileUpload(triplesFile4, pushEvent4.projectName)
      }
    }
  }

  private trait TestCase {
    val triplesFinder: TriplesFinder = mock[TriplesFinder]
    val fusekiConnector: FusekiConnector = stub[FusekiConnector]
    val logger: LoggingAdapter = stub[LoggingAdapter]
    val pushEventQueue = new PushEventQueue(
      triplesFinder,
      fusekiConnector,
      QueueConfig(BufferSize(1), TriplesFinderThreads(1)),
      logger
    )

    def givenGenerateTriples(event: PushEvent) = new {
      def returning(outcome: Future[Either[Exception, TriplesFile]]) =
        (triplesFinder.generateTriples(_: GitRepositoryUrl, _: CheckoutSha)(_: ExecutionContext))
          .expects(event.gitRepositoryUrl, event.checkoutSha, implicitly[ExecutionContext])
          .returning(outcome)
    }

    def givenTriplesFileUpload(triplesFile: TriplesFile, projectName: ProjectName) = new {
      def returning(outcome: Future[Unit]) =
        (fusekiConnector.uploadFile(_: TriplesFile, _: ProjectName)(_: ExecutionContext))
          .when(triplesFile, projectName, implicitly[ExecutionContext])
          .returning(outcome)
    }

    def verifyGeneratingTriplesError(pushEvent: PushEvent, exception: Exception) =
      (logger.error(_: String))
        .verify(s"Generating triples for $pushEvent failed: ${exception.getMessage}")

    def verifyUploadingTriplesFileError(pushEvent: PushEvent, exception: Exception) =
      (logger.error(_: String))
        .verify(s"Uploading triples for $pushEvent failed: ${exception.getMessage}")

    def verifyTriplesFileUpload(triplesFile: TriplesFile, projectName: ProjectName) =
      (fusekiConnector.uploadFile(_: TriplesFile, _: ProjectName)(_: ExecutionContext))
        .verify(triplesFile, projectName, implicitly[ExecutionContext])
  }
}
