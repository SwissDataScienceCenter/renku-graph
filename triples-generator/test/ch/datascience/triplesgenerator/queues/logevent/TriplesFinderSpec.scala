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

import java.io.InputStream

import ammonite.ops.{CommandResult, root}
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import ch.datascience.triplesgenerator.queues.logevent.LogEventQueue.{CommitWithParent, CommitWithoutParent}
import org.scalacheck.Gen
import org.scalamock.function.{MockFunction0, MockFunction1}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import os.Path

import scala.concurrent.ExecutionContext.Implicits.global

class TriplesFinderSpec extends WordSpec with MockFactory with ScalaFutures with IntegrationPatience {

  "generateTriples" should {

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "call 'renku log' without --revision when no parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(rdfTriplesStream)

      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(rdfTriples)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Right(rdfTriples)
    }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "call 'renku log' with --revision when there's a parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {
      val commitWithParent = toCommitWithParent(commitWithoutParent)

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)

      (renku
        .log(_: CommitWithParent, _: Path)(_: (CommitWithParent, Path) => CommandResult))
        .expects(commitWithParent, repositoryDirectory, renku.commitWithParentTriplesFinder)
        .returning(rdfTriplesStream)

      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(rdfTriples)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithParent).futureValue shouldBe Right(rdfTriples)
    }

    "return an error if create a temp directory fails" in new TestCase {
      val exception = new Exception("message")
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .throwing(exception)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Left(exception)
    }

    "return an error if cloning the repo fails" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      val exception = new Exception("message")
      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .throwing(exception)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Left(exception)
    }

    "return an error if checking out the sha fails" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)

      val exception = new Exception("message")
      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .throwing(exception)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Left(exception)
    }

    "return an error if calling 'renku log' fails" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)

      val exception = new Exception("message")
      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .throwing(exception)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Left(exception)
    }

    "return an error if converting the rdf triples stream to rdf triples fails" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(rdfTriplesStream)

      val exception = new Exception("message")
      toRdfTriples
        .expects(rdfTriplesStream)
        .throwing(exception)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Left(exception)
    }

    "return an error if removing the temp folder fails" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(rdfTriplesStream)

      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(rdfTriples)

      val exception = new Exception("message")
      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)
        .throwing(exception)

      (file
        .removeSilently(_: Path))
        .expects(repositoryDirectory)

      triplesFinder.generateTriples(commitWithoutParent).futureValue shouldBe Left(exception)
    }
  }

  private trait TestCase {
    val gitLabUrl:        ServiceUrl  = serviceUrls.generateOne
    val repositoryName:   String      = nonEmptyStrings().generateOne
    val projectPath:      ProjectPath = ProjectPath(s"user/$repositoryName")
    val gitRepositoryUrl: ServiceUrl  = gitLabUrl / s"$projectPath.git"
    val commitWithoutParent @ CommitWithoutParent(commitId, _) =
      CommitWithoutParent(commitIds.generateOne, projectPath)

    def toCommitWithParent(commitWithoutParent: CommitWithoutParent): CommitWithParent =
      CommitWithParent(
        commitWithoutParent.id,
        commitIds.generateOne,
        commitWithoutParent.projectPath
      )

    val pathDifferentiator: Int = Gen.choose(1, 100).generateOne

    val workDirectory:       Path        = root / "tmp"
    val repositoryDirectory: Path        = workDirectory / s"$repositoryName-$pathDifferentiator"
    val rdfTriplesStream:    InputStream = mock[InputStream]
    val rdfTriples:          RDFTriples  = rdfTriplesSets.generateOne

    val file:         Commands.File                          = mock[Commands.File]
    val git:          Commands.Git                           = mock[Commands.Git]
    val renku:        Commands.Renku                         = mock[Commands.Renku]
    val randomLong:   MockFunction0[Long]                    = mockFunction[Long]
    val toRdfTriples: MockFunction1[InputStream, RDFTriples] = mockFunction[InputStream, RDFTriples]
    randomLong.expects().returning(pathDifferentiator)
    val triplesFinder = new TriplesFinder(file, git, renku, gitLabUrl, toRdfTriples, randomLong)
  }
}
