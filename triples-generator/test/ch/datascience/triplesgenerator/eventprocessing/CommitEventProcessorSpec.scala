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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.{CommitId, ProjectPath}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class CommitEventProcessorSpec extends WordSpec with MockFactory {

  "toTriplesAndStore" should {

    "succeed if a commit event in Json can be deserialised, turn into triples and all stored in Jena successfully" in new TestCase {

      val commits = commitsLists().generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: String))
        .expects(eventJson)
        .returning(context.pure(commits))

      commits map succeedTriplesAndUploading

      eventProcessor.toTriplesAndStore(eventJson) shouldBe context.unit

      commits.toList foreach logSuccess
    }

    "succeed if a commit event in Json can be deserialised, turn into triples and all stored in Jena successfully " +
      "even if some failed in different stages" in new TestCase {

      val commits                                         = commitsLists(Gen.const(4)).generateOne
      val commit1 +: commit2 +: commit3 +: commit4 +: Nil = commits.toList

      (eventsDeserialiser
        .deserialiseToCommitEvents(_: String))
        .expects(eventJson)
        .returning(context.pure(commits))

      succeedTriplesAndUploading(commit1)

      val exception2 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit))
        .expects(commit2)
        .returning(context.raiseError(exception2))

      val triples3   = rdfTriplesSets.generateOne
      val exception3 = exceptions.generateOne
      (triplesFinder
        .generateTriples(_: Commit))
        .expects(commit3)
        .returning(context.pure(triples3))
      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples3)
        .returning(context.raiseError(exception3))

      succeedTriplesAndUploading(commit4)

      eventProcessor.toTriplesAndStore(eventJson) shouldBe context.unit

      logSuccess(commit1)
      logError(commit2, exception2)
      logError(commit3, exception3)
      logSuccess(commit4)
    }

    "succeed but log an error if CommitEvent deserialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventsDeserialiser
        .deserialiseToCommitEvents(_: String))
        .expects(eventJson)
        .returning(context.raiseError(exception))

      eventProcessor.toTriplesAndStore(eventJson) shouldBe context.unit

      logger.loggedOnly(Error("Commit Event deserialisation failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val eventJson = nonEmptyStrings().generateOne

    val eventsDeserialiser = mock[TryCommitEventsDeserialiser]
    val triplesFinder      = mock[TryTriplesFinder]
    val fusekiConnector    = mock[TryFusekiConnector]
    val logger             = TestLogger[Try]()
    val eventProcessor = new CommitEventProcessor[Try](
      eventsDeserialiser,
      triplesFinder,
      fusekiConnector,
      logger
    )

    def succeedTriplesAndUploading(commit: Commit): Unit = {
      val triples = rdfTriplesSets.generateOne
      (triplesFinder
        .generateTriples(_: Commit))
        .expects(commit)
        .returning(context.pure(triples))

      (fusekiConnector
        .upload(_: RDFTriples))
        .expects(triples)
        .returning(context.unit)
    }

    val logSuccess: Commit => Unit = {
      case CommitWithoutParent(id, projectPath) =>
        logger.logged(Info(s"Commit Event id: $id, project: $projectPath processed"))
      case CommitWithParent(id, parentId, projectPath) =>
        logger.logged(Info(s"Commit Event id: $id, project: $projectPath, parentId: $parentId processed"))
    }

    def logError(commit: Commit, exception: Exception): Unit = commit match {
      case CommitWithoutParent(id, projectPath) =>
        logger.logged(Error(s"Commit Event id: $id, project: $projectPath failed", exception))
      case CommitWithParent(id, parentId, projectPath) =>
        logger.logged(Error(s"Commit Event id: $id, project: $projectPath, parentId: $parentId failed", exception))
    }
  }

  private def commits(commitId: CommitId, projectPath: ProjectPath): Gen[Commit] =
    for {
      maybeParentId <- Gen.option(commitIds)
    } yield
      maybeParentId match {
        case None           => CommitWithoutParent(commitId, projectPath)
        case Some(parentId) => CommitWithParent(commitId, parentId, projectPath)
      }

  private def commitsLists(sizeGen: Gen[Int] = positiveInts(max = 5)): Gen[NonEmptyList[Commit]] =
    for {
      commitId    <- commitIds
      projectPath <- projectPaths
      size        <- sizeGen
      commits     <- Gen.listOfN(size, commits(commitId, projectPath))
    } yield NonEmptyList.fromListUnsafe(commits)
}
