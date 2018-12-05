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

package ch.datascience.triplesgenerator.queues.logevent

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ ActorMaterializer, Materializer }
import ch.datascience.config.{ AsyncParallelism, BufferSize }
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.tools.AsyncTestCase
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{ Eventually, IntegrationPatience, ScalaFutures }
import play.api.Logger
import play.api.libs.json.JsValue
import play.api.libs.json.Json._

import scala.concurrent.ExecutionContext.Implicits.{ global => ec }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class LogEventQueueSpec
  extends WordSpec
  with MockFactory
  with Eventually
  with ScalaFutures
  with IntegrationPatience {

  import ch.datascience.triplesgenerator.queues.logevent.LogEventQueue._

  "LogEventQueue" should {

    "offer an event from the source to the queue " +
      "and trigger triples generation and upload to Fuseki" in new TestCase {

        val ( commitEventJson, commitInfo, rdfTriples ) = generateData

        givenGenerateTriples( commitInfo ) returning Future.successful( Right( rdfTriples ) )
        givenTriplesUpload( rdfTriples ) returningAndFinishingTest Future.successful( () )

        sendToLogEventQueue( Success( commitEventJson ) )

        verifyTriplesUpload( rdfTriples )

        waitForAsyncProcess( implicitly[PatienceConfig] )
      }

    "offer an event from the source to the queue and log an error " +
      "when generating the triples returns an error" in new TestCase {

        val ( commitEventJson, commitInfo, rdfTriples ) = generateData

        val exception: Exception = exceptions.generateOne
        givenGenerateTriples( commitInfo ) returningAndFinishingTest Future.successful( Left( exception ) )

        sendToLogEventQueue( Success( commitEventJson ) )

        ( fusekiConnector.upload( _: RDFTriples )( _: ExecutionContext ) )
          .verify( *, * )
          .never()

        waitForAsyncProcess
      }

    "offer an event from the source to the queue and log an error " +
      "when uploading the triples returns an error" in new TestCase {

        val ( commitEventJson, commitInfo, rdfTriples ) = generateData

        givenGenerateTriples( commitInfo ) returning Future.successful( Right( rdfTriples ) )

        val exception: Exception = exceptions.generateOne
        givenTriplesUpload( rdfTriples ) returningAndFinishingTest Future.failed( exception )

        sendToLogEventQueue( Success( commitEventJson ) )

        verifyTriplesUpload( rdfTriples )

        waitForAsyncProcess
      }

    "not offer a failure to the queue but log an error" in new TestCase {

      val exception = exceptions.generateOne

      sendToLogEventQueue( Failure( exception ) )

      eventually {
        ( fusekiConnector.upload( _: RDFTriples )( _: ExecutionContext ) )
          .verify( *, * )
          .never()
      }
    }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val ( commitEventJson1, commitInfo1, rdfTriples1 ) = generateData
      givenGenerateTriples( commitInfo1 ) returning Future.successful( Right( rdfTriples1 ) )
      givenTriplesUpload( rdfTriples1 ) returning Future.successful( () )

      val ( commitEventJson2, commitInfo2, _ ) = generateData
      val exception2: Exception = exceptions.generateOne
      givenGenerateTriples( commitInfo2 ) returning Future.successful( Left( exception2 ) )

      val ( commitEventJson3, commitInfo3, rdfTriples3 ) = generateData
      val exception3: Exception = exceptions.generateOne
      givenGenerateTriples( commitInfo3 ) returning Future.successful( Right( rdfTriples3 ) )
      givenTriplesUpload( rdfTriples3 ) returning Future.failed( exception3 )

      val exception4: Exception = exceptions.generateOne

      val ( commitEventJson5, commitInfo5, rdfTriples5 ) = generateData
      givenGenerateTriples( commitInfo5 ) returning Future.successful( Right( rdfTriples5 ) )
      givenTriplesUpload( rdfTriples5 ) returningAndFinishingTest Future.successful( () )

      sendToLogEventQueue(
        Success( commitEventJson1 ),
        Success( commitEventJson2 ),
        Success( commitEventJson3 ),
        Failure( exception4 ),
        Success( commitEventJson5 )
      )

      verifyTriplesUpload( rdfTriples1 )
      verifyTriplesUpload( rdfTriples5 )

      waitForAsyncProcess
    }
  }

  private trait TestCase extends AsyncTestCase {

    private implicit val system: ActorSystem = ActorSystem( "MyTest" )
    private implicit val materializer: Materializer = ActorMaterializer()

    val triplesFinder: TriplesFinder = mock[TriplesFinder]
    val fusekiConnector: FusekiConnector = stub[FusekiConnector]

    def sendToLogEventQueue( events: Try[JsValue]* ): Unit = {

      val logEventsSource: Source[Try[JsValue], Future[Done]] =
        Source[Try[JsValue]]( events.toList )
          .mapMaterializedValue( _ => Future.successful( Done ) )

      new LogEventQueue(
        QueueConfig(
          bufferSize           = BufferSize( 1 ),
          triplesFinderThreads = AsyncParallelism( 1 ),
          fusekiUploadThreads  = AsyncParallelism( 1 )
        ),
        logEventsSource,
        triplesFinder,
        fusekiConnector,
        Logger
      )
    }

    def generateData = {
      val commitEvent: CommitEvent = commitEvents.generateOne
      val commitEventJson: JsValue = toJson( commitEvent )
      val commitInfo: CommitInfo = CommitInfo(
        commitEvent.id,
        commitEvent.project.path
      )
      val rdfTriples: RDFTriples = rdfTriplesSets.generateOne
      ( commitEventJson, commitInfo, rdfTriples )
    }

    def givenGenerateTriples( commitInfo: CommitInfo ) = new {
      private val stubbing =
        ( triplesFinder.generateTriples( _: ProjectPath, _: CommitId )( _: ExecutionContext ) )
          .expects( commitInfo.projectPath, commitInfo.id, implicitly[ExecutionContext] )

      def returning( outcome: Future[Either[Exception, RDFTriples]] ): Unit =
        stubbing
          .returning( outcome )

      def returningAndFinishingTest( outcome: Future[Either[Exception, RDFTriples]] ): Unit =
        stubbing
          .returning( outcome )
          .onCall { _ =>
            asyncProcessFinished()
            outcome
          }
    }

    def givenTriplesUpload( rdfTriples: RDFTriples ) = new {
      private val stubbing =
        ( fusekiConnector.upload( _: RDFTriples )( _: ExecutionContext ) )
          .when( rdfTriples, ec )

      def returning( outcome: Future[Unit] ): Unit =
        stubbing
          .returning( outcome )

      def returningAndFinishingTest( outcome: Future[Unit] ): Unit =
        stubbing
          .returning( outcome )
          .onCall { _ =>
            asyncProcessFinished()
            outcome
          }
    }

    def verifyTriplesUpload( rdfTriples: RDFTriples ) =
      ( fusekiConnector.upload( _: RDFTriples )( _: ExecutionContext ) )
        .verify( rdfTriples, ec )
  }
}
