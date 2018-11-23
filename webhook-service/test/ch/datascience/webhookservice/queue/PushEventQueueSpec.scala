/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queue

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.QueueOfferResult.Enqueued
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.{ CheckoutSha, GitRepositoryUrl, PushEvent }
import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{ Eventually, IntegrationPatience, ScalaFutures }
import play.api.LoggerLike

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ ExecutionContext, Future }

class PushEventQueueSpec
  extends WordSpec
  with MixedMockFactory
  with Eventually
  with ScalaFutures
  with IntegrationPatience {

  "offer" should {

    "return Enqueued and trigger triples generation and upload to Fuseki" in new TestCase {
      val pushEvent: PushEvent = pushEvents.generateOne
      val rdfTriples: RDFTriples = rdfTriplesSets.generateOne

      givenGenerateTriples( pushEvent ) returning Future.successful( Right( rdfTriples ) )
      givenTriplesFileUpload( rdfTriples ) returning Future.successful( () )

      pushEventQueue.offer( pushEvent ).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload( rdfTriples )
      }
    }

    "return Enqueued and log an error " +
      "when generating the triples returns an error" in new TestCase {
        val pushEvent: PushEvent = pushEvents.generateOne
        val exception = new Exception( "error" )

        givenGenerateTriples( pushEvent ) returning Future.successful( Left( exception ) )

        pushEventQueue.offer( pushEvent ).futureValue shouldBe Enqueued

        eventually {
          verifyGeneratingTriplesError( pushEvent, exception )

          ( fusekiConnector.uploadFile( _: RDFTriples )( _: ExecutionContext ) )
            .verify( *, * )
            .never()
        }
      }

    "return Enqueued and log an error " +
      "when uploading the triples returns an error" in new TestCase {
        val pushEvent: PushEvent = pushEvents.generateOne
        val rdfTriples: RDFTriples = rdfTriplesSets.generateOne

        givenGenerateTriples( pushEvent ) returning Future.successful( Right( rdfTriples ) )

        val exception = new Exception( "error" )
        givenTriplesFileUpload( rdfTriples ) returning Future.failed( exception )

        pushEventQueue.offer( pushEvent ).futureValue shouldBe Enqueued

        eventually {
          verifyTriplesFileUpload( rdfTriples )
          verifyUploadingTriplesFileError( pushEvent, exception )
        }
      }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val pushEvent1: PushEvent = pushEvents.generateOne
      val rdfTriples1: RDFTriples = rdfTriplesSets.generateOne
      givenGenerateTriples( pushEvent1 ) returning Future.successful( Right( rdfTriples1 ) )
      givenTriplesFileUpload( rdfTriples1 ) returning Future.successful( () )

      val pushEvent2: PushEvent = pushEvents.generateOne
      val exception2 = new Exception( "error 2" )
      givenGenerateTriples( pushEvent2 ) returning Future.successful( Left( exception2 ) )

      val pushEvent3: PushEvent = pushEvents.generateOne
      val rdfTriples3: RDFTriples = rdfTriplesSets.generateOne
      val exception3 = new Exception( "error 3" )
      givenGenerateTriples( pushEvent3 ) returning Future.successful( Right( rdfTriples3 ) )
      givenTriplesFileUpload( rdfTriples3 ) returning Future.failed( exception3 )

      val pushEvent4: PushEvent = pushEvents.generateOne
      val rdfTriples4: RDFTriples = rdfTriplesSets.generateOne
      givenGenerateTriples( pushEvent4 ) returning Future.successful( Right( rdfTriples4 ) )
      givenTriplesFileUpload( rdfTriples4 ) returning Future.successful( () )

      pushEventQueue.offer( pushEvent1 ).futureValue shouldBe Enqueued
      pushEventQueue.offer( pushEvent2 ).futureValue shouldBe Enqueued
      pushEventQueue.offer( pushEvent3 ).futureValue shouldBe Enqueued
      pushEventQueue.offer( pushEvent4 ).futureValue shouldBe Enqueued

      eventually {
        verifyTriplesFileUpload( rdfTriples1 )
        verifyTriplesFileUpload( rdfTriples4 )
      }
    }
  }

  private trait TestCase {
    private implicit val system = ActorSystem( "MyTest" )
    private implicit val materializer = ActorMaterializer()

    val triplesFinder: TriplesFinder = mock[TriplesFinder]
    val fusekiConnector: FusekiConnector = stub[FusekiConnector]
    val logger = Proxy.stub[LoggerLike]
    val pushEventQueue = new PushEventQueue(
      triplesFinder,
      fusekiConnector,
      QueueConfig( BufferSize( 1 ), TriplesFinderThreads( 1 ), FusekiUploadThreads( 1 ) ),
      logger
    )

    def givenGenerateTriples( event: PushEvent ) = new {
      def returning( outcome: Future[Either[Exception, RDFTriples]] ) =
        ( triplesFinder.generateTriples( _: GitRepositoryUrl, _: CheckoutSha )( _: ExecutionContext ) )
          .expects( event.gitRepositoryUrl, event.checkoutSha, implicitly[ExecutionContext] )
          .returning( outcome )
    }

    def givenTriplesFileUpload( rdfTriples: RDFTriples ) = new {
      def returning( outcome: Future[Unit] ) =
        ( fusekiConnector.uploadFile( _: RDFTriples )( _: ExecutionContext ) )
          .when( rdfTriples, implicitly[ExecutionContext] )
          .returning( outcome )
    }

    def verifyGeneratingTriplesError( pushEvent: PushEvent, exception: Exception ) =
      logger.verify( 'error )(
        argAssert { ( message: () => String ) =>
          message() shouldBe s"Generating triples for $pushEvent failed: ${exception.getMessage}"
        }, *
      )

    def verifyUploadingTriplesFileError( pushEvent: PushEvent, exception: Exception ) =
      logger.verify( 'error )(
        argAssert { ( message: () => String ) =>
          message() shouldBe s"Uploading triples for $pushEvent failed: ${exception.getMessage}"
        }, *
      )

    def verifyTriplesFileUpload( rdfTriples: RDFTriples ) =
      ( fusekiConnector.uploadFile( _: RDFTriples )( _: ExecutionContext ) )
        .verify( rdfTriples, implicitly[ExecutionContext] )
  }
}
