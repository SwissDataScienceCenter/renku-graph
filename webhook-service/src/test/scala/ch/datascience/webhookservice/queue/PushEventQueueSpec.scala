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

    "return Enqueued so finding the triplets and storing them in the Jena is triggered" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val graph: Graph = mock[Graph]

      givenFindTriplets(pushEvent) returning Future.successful(Right(graph))
      givenGraphPersist(graph) returning Future.successful()

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyTripletsPersisted(graph)
      }
    }

    "return Enqueued and log an error " +
      "when finding the triplets returns an error" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val exception = new Exception("error")

      givenFindTriplets(pushEvent) returning Future.successful(Left(exception))

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyFindingTripletsError(pushEvent, exception)

        (jenaConnector.persist(_: Graph))
          .verify(*)
          .never()
      }
    }

    "return Enqueued and log an error " +
      "when storing the triplets returns an error" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val graph: Graph = mock[Graph]

      givenFindTriplets(pushEvent) returning Future.successful(Right(graph))

      val exception = new Exception("error")
      givenGraphPersist(graph) returning Future.failed(exception)

      pushEventQueue.offer(pushEvent).futureValue shouldBe Enqueued

      eventually {
        verifyTripletsPersisted(graph)
        verifyPersistingTripletsError(pushEvent, exception)
      }
    }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val pushEvent1: PushEvent = pushEvents.generateOne
      val graph1: Graph = mock[Graph]
      givenFindTriplets(pushEvent1) returning Future.successful(Right(graph1))
      givenGraphPersist(graph1) returning Future.successful()

      val pushEvent2: PushEvent = pushEvents.generateOne
      val exception2 = new Exception("error 2")
      givenFindTriplets(pushEvent2) returning Future.successful(Left(exception2))

      val pushEvent3: PushEvent = pushEvents.generateOne
      val graph3: Graph = mock[Graph]
      val exception3 = new Exception("error 3")
      givenFindTriplets(pushEvent3) returning Future.successful(Right(graph3))
      givenGraphPersist(graph3) returning Future.failed(exception3)

      val pushEvent4: PushEvent = pushEvents.generateOne
      val graph4: Graph = mock[Graph]
      givenFindTriplets(pushEvent4) returning Future.successful(Right(graph4))
      givenGraphPersist(graph4) returning Future.successful()

      pushEventQueue.offer(pushEvent1).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent2).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent3).futureValue shouldBe Enqueued
      pushEventQueue.offer(pushEvent4).futureValue shouldBe Enqueued

      eventually {
        verifyTripletsPersisted(graph1)

        verifyFindingTripletsError(pushEvent2, exception2)

        verifyPersistingTripletsError(pushEvent3, exception3)

        verifyTripletsPersisted(graph4)
      }
    }
  }

  private trait TestCase {
    val tripletsFinder: TripletsFinder = mock[TripletsFinder]
    val jenaConnector: JenaConnector = stub[JenaConnector]
    val logger: LoggingAdapter = stub[LoggingAdapter]
    val pushEventQueue = new PushEventQueue(
      tripletsFinder,
      jenaConnector,
      QueueConfig(BufferSize(1), TripletsFinderThreads(1)),
      logger
    )

    def givenFindTriplets(event: PushEvent) = new {
      def returning(outcome: Future[Either[Exception, Graph]]) =
        (tripletsFinder.findRdfGraph(_: GitRepositoryUrl, _: CheckoutSha)(_: ExecutionContext))
          .expects(event.gitRepositoryUrl, event.checkoutSha, implicitly[ExecutionContext])
          .returning(outcome)
    }

    def givenGraphPersist(graph: Graph) = new {
      def returning(outcome: Future[Unit]) =
        (jenaConnector.persist(_: Graph))
          .when(graph)
          .returning(outcome)
    }

    def verifyFindingTripletsError(pushEvent: PushEvent, exception: Exception) =
      (logger.error(_: String))
        .verify(s"Finding triplets for $pushEvent failed: ${exception.getMessage}")

    def verifyPersistingTripletsError(pushEvent: PushEvent, exception: Exception) =
      (logger.error(_: String))
        .verify(s"Persisting triplets for $pushEvent failed: ${exception.getMessage}")

    def verifyTripletsPersisted(graph: Graph) =
      (jenaConnector.persist(_: Graph))
        .verify(graph)
  }
}
