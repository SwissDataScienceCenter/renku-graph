/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import cats.effect._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.commands.EventLogFetch
import ch.datascience.dbeventlog.{EventBody, EventLogDB}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import com.typesafe.config.ConfigFactory
import doobie.util.transactor.Transactor
import org.scalacheck.Gen.listOfN
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.{higherKinds, postfixOps, reflectiveCalls}

class DbEventProcessorRunnerSpec extends WordSpec with Eventually with IntegrationPatience with MockFactory {

  "event source" should {

    "send every new event fetched from db to the registered processor and process them concurrently" in new TestCase {
      val events = listOfN(1000, eventBodies).generateOne

      eventLogFetch.addEventsToReturn(events)

      val accumulator = new ConcurrentHashMap[EventBody, Long]()
      def processor(event: EventBody): IO[Unit] = {
        accumulator.put(event, Thread.currentThread().getId)
        IO.unit
      }

      eventSourceWith(processor).run.unsafeRunAsyncAndForget()

      eventually {
        accumulator.keySet().asScala shouldBe events.toSet
      }

      val subsequentEvent = eventBodies.generateOne
      eventLogFetch.addEventsToReturn(Seq(subsequentEvent))

      eventually {
        accumulator.keySet().asScala shouldBe (events :+ subsequentEvent).toSet
      }

      logger.loggedOnly(Info("Waiting for new events"))
    }

    "continue if there is an error during processing" in new TestCase {
      val eventBody1 = eventBodies.generateOne
      val eventBody2 = eventBodies.generateOne
      val eventBody3 = eventBodies.generateOne

      eventLogFetch.addEventsToReturn(Seq(eventBody1, eventBody2, eventBody3))

      val accumulator = new ConcurrentHashMap[EventBody, Long]()
      def processor(event: EventBody): IO[Unit] =
        if (event == eventBody2)
          IO.raiseError(new Exception("error during processing eventBody2"))
        else {
          accumulator.put(event, Thread.currentThread().getId)
          IO.unit
        }

      eventSourceWith(processor).run.unsafeRunAsyncAndForget()

      eventually {
        accumulator.keySet().asScala shouldBe Set(eventBody1, eventBody3)
      }

      val eventBody4 = eventBodies.generateOne
      eventLogFetch.addEventsToReturn(Seq(eventBody4))

      eventually {
        accumulator.keySet().asScala shouldBe Set(eventBody1, eventBody3, eventBody4)
      }

      logger.loggedOnly(Info("Waiting for new events"))
    }

    "continue if there is an error while checking events in the queue" in new TestCase {

      val isEventException  = exceptions.generateOne
      val popEventException = exceptions.generateOne
      val eventBody         = eventBodies.generateOne
      val failingEvenLog = new TestEventLogFetch(
        List(IO.raiseError[Boolean](isEventException), IO.pure(true)),
        List(IO.raiseError[Option[EventBody]](popEventException))
      )

      val accumulator = new ConcurrentHashMap[EventBody, Long]()
      def processor(event: EventBody): IO[Unit] = {
        accumulator.put(event, Thread.currentThread().getId)
        IO.unit
      }

      eventSourceWith(processor, eventLogFetch = failingEvenLog).run.unsafeRunAsyncAndForget()

      eventually {
        accumulator.keySet().asScala shouldBe Set.empty

        logger.loggedOnly(
          Error("Couldn't access Event Log", isEventException),
          Error("Couldn't access Event Log", popEventException),
          Info("Waiting for new events")
        )
      }

      failingEvenLog.addEventsToReturn(Seq(eventBody))

      eventually {
        accumulator.keySet().asScala shouldBe Set(eventBody)
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {
    val logger         = TestLogger[IO]()
    val eventLogFetch  = new TestEventLogFetch()
    private val config = ConfigFactory.parseMap(Map("generation-processes-number" -> 5).asJava)

    def eventSourceWith(processor:     EventProcessor[IO],
                        eventLogFetch: EventLogFetch[IO] = eventLogFetch): EventProcessorRunner[IO] = {
      val eventRunner = DbEventProcessorRunner(_, eventLogFetch, config, logger)
      new EventsSource[IO](eventRunner).withEventsProcessor(processor).unsafeRunSync()
    }
  }

  private class TestDbTransactor(transactor: Transactor.Aux[IO, _]) extends DbTransactor[IO, EventLogDB](transactor)

  private class TestEventLogFetch(isEventEvents:  List[IO[Boolean]]           = Nil,
                                  popEventEvents: List[IO[Option[EventBody]]] = Nil)
      extends EventLogFetch[IO] {
    private val isEventEventsQueue  = new ConcurrentLinkedQueue[IO[Boolean]](isEventEvents.asJava)
    private val popEventEventsQueue = new ConcurrentLinkedQueue[IO[Option[EventBody]]](popEventEvents.asJava)

    def addEventsToReturn(events: Seq[EventBody]) = {
      isEventEventsQueue addAll events.map(_ => IO.pure(true)).asJava
      popEventEventsQueue addAll events.map(event => IO.pure(Option(event))).asJava
    }

    override def isEventToProcess: IO[Boolean] =
      Option(isEventEventsQueue.poll()) getOrElse IO.pure(false)

    override def popEventToProcess: IO[Option[EventBody]] =
      Option(popEventEventsQueue.poll()) getOrElse IO.pure(None)
  }
}
