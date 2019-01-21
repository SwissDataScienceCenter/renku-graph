/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import akka.stream.{ActorMaterializer, Materializer}
import ch.datascience.config.{AsyncParallelism, BufferSize}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.tools.AsyncTestCase
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import play.api.Logger
import play.api.libs.json.JsValue
import play.api.libs.json.Json._

import scala.concurrent.ExecutionContext.Implicits.{global => ec}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class LogEventQueueSpec extends WordSpec with MockFactory with Eventually with ScalaFutures with IntegrationPatience {

  import ch.datascience.triplesgenerator.queues.logevent.LogEventQueue._

  "LogEventQueue" should {

    "offer an event from the source to the queue " +
      "and trigger triples generation and upload to Fuseki " +
      "when there are no parent commits" in new TestCase {

      val (commitEventJson, commit, rdfTriples) = commitAndTriples

      givenGenerateTriples(commit) returning Future.successful(Right(rdfTriples))

      givenTriplesUpload(rdfTriples) returningAndFinishingTest Future.successful(())

      sendToLogEventQueue(Success(commitEventJson))

      verifyTriplesUpload(rdfTriples)

      waitForAsyncProcess(implicitly[PatienceConfig])
    }

    "offer an event from the source to the queue " +
      "and trigger triples generation and upload to Fuseki " +
      "when there's more than one parent commit" in new TestCase {

      val (commitEventJson, commits, rdfTriplesSeq) = commitsAndTriples(withAtLeastOneParent).generateOne

      commits.zip(rdfTriplesSeq) foreach {
        case (commit, rdfTriples) =>
          givenGenerateTriples(commit) returning Future.successful(Right(rdfTriples))

          if (rdfTriples != rdfTriplesSeq.last)
            givenTriplesUpload(rdfTriples) returning Future.successful(())
          else
            givenTriplesUpload(rdfTriples) returningAndFinishingTest Future.successful(())
      }

      sendToLogEventQueue(Success(commitEventJson))

      rdfTriplesSeq foreach verifyTriplesUpload

      waitForAsyncProcess(implicitly[PatienceConfig])
    }

    "offer an event from the source to the queue and log an error " +
      "when generating the triples returns an error" in new TestCase {

      val (commitEventJson, commit, rdfTriples) = commitAndTriples

      val exception: Exception = exceptions.generateOne
      givenGenerateTriples(commit) returningAndFinishingTest Future.successful(Left(exception))

      sendToLogEventQueue(Success(commitEventJson))

      (fusekiConnector
        .upload(_: RDFTriples)(_: ExecutionContext))
        .verify(*, *)
        .never()

      waitForAsyncProcess
    }

    "offer an event from the source to the queue and log an error " +
      "when uploading the triples returns an error" in new TestCase {

      val (commitEventJson, commit, rdfTriples) = commitAndTriples

      givenGenerateTriples(commit) returning Future.successful(Right(rdfTriples))

      val exception: Exception = exceptions.generateOne
      givenTriplesUpload(rdfTriples) returningAndFinishingTest Future.failed(exception)

      sendToLogEventQueue(Success(commitEventJson))

      verifyTriplesUpload(rdfTriples)

      waitForAsyncProcess
    }

    "not offer a failure to the queue but log an error" in new TestCase {

      val exception = exceptions.generateOne

      sendToLogEventQueue(Failure(exception))

      eventually {
        (fusekiConnector
          .upload(_: RDFTriples)(_: ExecutionContext))
          .verify(*, *)
          .never()
      }
    }

    "not kill the queue when multiple events are offered and some of them fail" in new TestCase {
      val (commitEventJson1, commit1, rdfTriples1) = commitAndTriples
      givenGenerateTriples(commit1) returning Future.successful(Right(rdfTriples1))
      givenTriplesUpload(rdfTriples1) returning Future.successful(())

      val (commitEventJson2, commit2, _) = commitAndTriples
      val exception2: Exception = exceptions.generateOne
      givenGenerateTriples(commit2) returning Future.successful(Left(exception2))

      val (commitEventJson3, commit3, rdfTriples3) = commitAndTriples
      val exception3: Exception = exceptions.generateOne
      givenGenerateTriples(commit3) returning Future.successful(Right(rdfTriples3))
      givenTriplesUpload(rdfTriples3) returning Future.failed(exception3)

      val exception4: Exception = exceptions.generateOne

      val (commitEventJson5, commit5, rdfTriples5) = commitAndTriples
      givenGenerateTriples(commit5) returning Future.successful(Right(rdfTriples5))
      givenTriplesUpload(rdfTriples5) returningAndFinishingTest Future.successful(())

      sendToLogEventQueue(
        Success(commitEventJson1),
        Success(commitEventJson2),
        Success(commitEventJson3),
        Failure(exception4),
        Success(commitEventJson5)
      )

      verifyTriplesUpload(rdfTriples1)
      verifyTriplesUpload(rdfTriples5)

      waitForAsyncProcess
    }
  }

  private trait TestCase extends AsyncTestCase {

    private implicit val system:       ActorSystem  = ActorSystem("MyTest")
    private implicit val materializer: Materializer = ActorMaterializer()

    val triplesFinder:   TriplesFinder   = mock[TriplesFinder]
    val fusekiConnector: FusekiConnector = stub[FusekiConnector]

    def sendToLogEventQueue(events: Try[JsValue]*): Unit = {

      val logEventsSource: Source[Try[JsValue], Future[Done]] =
        Source[Try[JsValue]](events.toList)
          .mapMaterializedValue(_ => Future.successful(Done))

      new LogEventQueue(
        QueueConfig(
          bufferSize           = BufferSize(1),
          triplesFinderThreads = AsyncParallelism(1),
          fusekiUploadThreads  = AsyncParallelism(1)
        ),
        logEventsSource,
        triplesFinder,
        fusekiConnector,
        Logger
      )
    }

    def commitsAndTriples(parentsGen: Gen[List[CommitId]]) =
      for {
        parentIds   <- parentsGen
        commitEvent <- commitEvents.map(event => event.copy(parents = parentIds))
        commitEventJson = toJson(commitEvent)

        commits = commitEvent.parents match {
          case Nil     => Seq(CommitWithoutParent(commitEvent.id, commitEvent.project.path))
          case parents => parents.map(CommitWithParent(commitEvent.id, _, commitEvent.project.path))
        }

        rdfTriplesSeq <- Gen.listOfN(commits.size, rdfTriplesSets)
      } yield (commitEventJson, commits, rdfTriplesSeq)

    def commitAndTriples = {
      val (commitEventJson, commits, rdfTriplesSeq) = commitsAndTriples(withNoParents).generateOne
      val (commit, rdfTriples) :: Nil               = commits.zip(rdfTriplesSeq)
      (commitEventJson, commit, rdfTriples)
    }

    val withAtLeastOneParent: Gen[List[CommitId]] = for {
      parentCommitsNumber <- nonNegativeInts(4)
      parents             <- Gen.listOfN(parentCommitsNumber, commitIds)
    } yield parents

    val withNoParents: Gen[List[CommitId]] = Gen.const(Nil)

    def givenGenerateTriples(commit: Commit) = new {
      private val stubbing =
        (triplesFinder
          .generateTriples(_: Commit)(_: ExecutionContext))
          .expects(commit, implicitly[ExecutionContext])

      def returning(outcome: Future[Either[Exception, RDFTriples]]): Unit =
        stubbing
          .returning(outcome)

      def returningAndFinishingTest(outcome: Future[Either[Exception, RDFTriples]]): Unit =
        stubbing
          .returning(outcome)
          .onCall { _ =>
            asyncProcessFinished()
            outcome
          }
    }

    def givenTriplesUpload(rdfTriples: RDFTriples) = new {
      private val stubbing =
        (fusekiConnector
          .upload(_: RDFTriples)(_: ExecutionContext))
          .when(rdfTriples, ec)

      def returning(outcome: Future[Unit]): Unit =
        stubbing
          .returning(outcome)

      def returningAndFinishingTest(outcome: Future[Unit]): Unit =
        stubbing
          .returning(outcome)
          .onCall { _ =>
            asyncProcessFinished()
            outcome
          }
    }

    def verifyTriplesUpload(rdfTriples: RDFTriples) =
      (fusekiConnector
        .upload(_: RDFTriples)(_: ExecutionContext))
        .verify(rdfTriples, ec)
  }
}
