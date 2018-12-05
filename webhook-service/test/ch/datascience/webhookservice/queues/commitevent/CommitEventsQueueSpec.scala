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

package ch.datascience.webhookservice.queues.commitevent

import java.nio.file.Path

import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.{ ActorMaterializer, Materializer }
import ch.datascience.config.BufferSize
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.CommitEvent
import ch.datascience.graph.events.EventsGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{ Eventually, IntegrationPatience, ScalaFutures }
import org.scalatest.prop.PropertyChecks
import play.api.libs.json.Json.toJson
import play.api.libs.json.{ JsValue, Json }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

class CommitEventsQueueSpec
  extends WordSpec
  with MixedMockFactory
  with Eventually
  with ScalaFutures
  with PropertyChecks
  with IntegrationPatience {

  "offer" should {

    "return Enqueued and trigger pushing offered CommitEvents to the Event Log" in new TestCase {

      val commitEventsList: List[CommitEvent] = commitEventsLists.generateOne

      commitEventsList.map { commitEvent =>
        commitEventsQueue.offer( commitEvent ).futureValue shouldBe Enqueued
      }

      eventually {
        sunkEvents should contain allElementsOf commitEventsList.map( toJson[CommitEvent] )
      }
    }
  }

  private trait TestCase {
    private implicit val system: ActorSystem = ActorSystem( "MyTest" )
    private implicit val materializer: Materializer = ActorMaterializer()

    val tempEventLogPath: Path = {
      import java.nio.file._
      val path = FileSystems.getDefault.getPath( "/tmp/renku-event.log" )
      Files.createFile( path ).toFile.deleteOnExit()
      path
    }

    val eventLogSinkProvider = new FileEventLogSinkProvider( tempEventLogPath )

    val commitEventsQueue = new CommitEventsQueue(
      QueueConfig( BufferSize( 1 ) ),
      eventLogSinkProvider.get
    )

    def sunkEvents: Stream[JsValue] =
      Source
        .fromFile( tempEventLogPath.toFile )
        .getLines()
        .map( Json.parse )
        .toStream
  }

  private val commitEventsLists: Gen[List[CommitEvent]] = for {
    eventsNumber <- positiveInts( max = 5 )
    events <- Gen.listOfN( eventsNumber, commitEvents )
  } yield events
}
