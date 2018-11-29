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

package ch.datascience.webhookservice.queues.logevent

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Source
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.webhookservice.config.BufferSize
import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{ Eventually, IntegrationPatience, ScalaFutures }
import org.scalatest.prop.PropertyChecks
import play.api.LoggerLike
import play.api.libs.json.JsValue
import play.api.libs.json.Json.toJson

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class LogEventQueueSpec
  extends WordSpec
  with MixedMockFactory
  with Eventually
  with ScalaFutures
  with PropertyChecks
  with IntegrationPatience {

  import ch.datascience.webhookservice.queues.commitevent.FileEventLogSinkProvider._

  "LogEventQueue" should {

    "offer an event from the source to the queue" in new TestCase {

      val commitEvent: JsValue = toJson( commitEvents.generateOne )

      sendToLogEventQueue( Success( commitEvent ) )

      eventually {
        verifyInfoLogged( s"Received event from Event Log: $commitEvent" )
      }
    }

    "not offer a failure to the queue but log an error" in new TestCase {

      val commitEvent: JsValue = toJson( commitEvents.generateOne )

      val exception = new Exception( "message" )

      sendToLogEventQueue( Failure( exception ) )

      eventually {
        verifyErrorLogged( "Received broken notification from Event Log" -> exception )
      }
    }
  }

  private trait TestCase {
    private implicit val system: ActorSystem = ActorSystem( "MyTest" )
    private implicit val materializer: Materializer = ActorMaterializer()

    private val logger = Proxy.stub[LoggerLike]

    def sendToLogEventQueue( event: Try[JsValue] ): Unit = {

      val logEventsSource: Source[Try[JsValue], Future[Done]] =
        Source
          .single( event )
          .mapMaterializedValue( _ => Future.successful( Done ) )

      new LogEventQueue(
        QueueConfig( BufferSize( 1 ) ),
        logEventsSource,
        logger
      )
    }

    def verifyInfoLogged( expectedMessages: String* ) = {
      var callsCounter = 0
      logger
        .verify( 'info )(
          argAssert { ( message: () => String ) =>
            message() shouldBe expectedMessages( callsCounter )
            callsCounter = callsCounter + 1
          },
          *
        )
        .repeat( expectedMessages.size )
    }

    def verifyErrorLogged( expectedMessages: ( String, Exception )* ) = {
      var callsCounter = 0
      logger.verify( 'error )(
        argAssert { ( message: () => String ) =>
          val ( expectedMessage, _ ) = expectedMessages( callsCounter )
          message() shouldBe expectedMessage
        },
        argAssert { ( throwable: () => Throwable ) =>
          val ( _, expectedException ) = expectedMessages( callsCounter )
          throwable() shouldBe expectedException
          callsCounter = callsCounter + 1
        },
        *
      )
        .repeat( expectedMessages.size )
    }
  }
}
