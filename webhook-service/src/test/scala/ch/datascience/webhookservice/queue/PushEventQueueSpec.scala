package ch.datascience.webhookservice.queue

import akka.event.LoggingAdapter
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.QueueOfferResult.Enqueued
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.{CheckoutSha, GitRepositoryUrl, PushEvent}
import org.apache.jena.graph.Graph
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{ExecutionContext, Future}

class PushEventQueueSpec extends WordSpec with MockFactory with ScalatestRouteTest with ScalaFutures {

  "offer" should {

    "return Enqueued when the given PushEvent is accepted and find the triplets for it" in new TestCase {
      (tripletsFinder.findRdfGraph(_: GitRepositoryUrl, _: CheckoutSha)(_: ExecutionContext))
        .expects(pushEvent.gitRepositoryUrl, pushEvent.checkoutSha, implicitly[ExecutionContext])
        .returning(Future.successful(Right(mock[Graph])))

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued
    }

    "return Enqueued and log an error " +
      "when the given PushEvent is accepted but finding triplets returns an error" in new TestCase {
      val exception = new Exception("error")
      (tripletsFinder.findRdfGraph(_: GitRepositoryUrl, _: CheckoutSha)(_: ExecutionContext))
        .expects(pushEvent.gitRepositoryUrl, pushEvent.checkoutSha, implicitly[ExecutionContext])
        .returning(Future.successful(Left(exception)))

      (logger.error(_:String))
        .expects(s"$pushEvent processing failed: ${exception.getMessage}")

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued
    }
  }

  private trait TestCase {
    val pushEvent: PushEvent = pushEvents.generateOne

    val tripletsFinder: TripletsFinder = mock[TripletsFinder]
    val logger: LoggingAdapter = mock[LoggingAdapter]
    val pushEventQueue = new PushEventQueue(
      tripletsFinder,
      QueueConfig(BufferSize(1), TripletsFinderThreads(1)),
      logger
    )
  }
}
