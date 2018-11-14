package ch.datascience.webhookservice

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.QueueOfferResult.{Enqueued, Failure}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.config.{BufferSize, TriplesFinderThreads}
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.triplets.TripletsFinder
import org.apache.jena.graph.Graph
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{ExecutionContext, Future}

class PushEventFlowSpec extends WordSpec with MockFactory with ScalatestRouteTest with ScalaFutures {

  "offer" should {

    "return Enqueued when the given PushEvent is accepted and find the triplets for it" in new TestCase {
      (tripletsFinder.findRdfGraph(_: GitRepositoryUrl, _: CheckoutSha)(_: ExecutionContext))
        .expects(pushEvent.gitRepositoryUrl, pushEvent.checkoutSha, implicitly[ExecutionContext])
        .returning(Future.successful(Right(mock[Graph])))

      pushEventFlow.offer(pushEvent).futureValue shouldBe Enqueued
    }
  }

  private trait TestCase {
    val pushEvent: PushEvent = pushEvents.generateOne

    val tripletsFinder: TripletsFinder = mock[TripletsFinder]
    val pushEventFlow = new PushEventFlow(
      tripletsFinder,
      QueueConfig(BufferSize(1), TriplesFinderThreads(1))
    )
  }
}
