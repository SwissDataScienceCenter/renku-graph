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

import java.io.InputStream

import ammonite.ops.{CommandResult, root}
import cats.effect.{ContextShift, IO}
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import os.Path

import scala.concurrent.ExecutionContext

class TriplesFinderSpec extends WordSpec with MockFactory {

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
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.pure(successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(IO.pure(rdfTriplesStream))

      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(IO.pure(rdfTriples))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync() shouldBe rdfTriples
    }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "call 'renku log' with --revision when there's a parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.pure(successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      val commitWithParent = toCommitWithParent(commitWithoutParent)
      (renku
        .log(_: CommitWithParent, _: Path)(_: (CommitWithParent, Path) => CommandResult))
        .expects(commitWithParent, repositoryDirectory, renku.commitWithParentTriplesFinder)
        .returning(IO.pure(rdfTriplesStream))

      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(IO.pure(rdfTriples))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      triplesFinder.generateTriples(commitWithParent).unsafeRunSync() shouldBe rdfTriples
    }

    "fail if temp directory creation fails" in new TestCase {

      val exception = exceptions.generateOne
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.raiseError(exception))

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail finding GitLabUrl fails" in new TestCase {
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      val exception = exceptions.generateOne
      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.raiseError(exception))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if cloning the repo fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      val exception = exceptions.generateOne
      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.raiseError(exception))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if checking out the sha fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.pure(successfulCommandResult))

      val exception = exceptions.generateOne
      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if calling 'renku log' fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.pure(successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      val exception = exceptions.generateOne
      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(IO.raiseError(exception))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if converting the rdf triples stream to rdf triples fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.pure(successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(IO.pure(rdfTriplesStream))

      val exception = exceptions.generateOne
      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(IO.raiseError(exception))

      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if removing the temp folder fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabUrlProvider.get _)
        .expects()
        .returning(IO.pure(gitLabUrl))

      (git
        .cloneRepo(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(IO.pure(successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(IO.pure(rdfTriplesStream))

      toRdfTriples
        .expects(rdfTriplesStream)
        .returning(IO.pure(rdfTriples))

      val exception = exceptions.generateOne
      (file
        .delete(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.raiseError(exception))
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesFinder.generateTriples(commitWithoutParent).unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {
    val successfulCommandResult = CommandResult(exitCode = 0, chunks = Nil)

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

    class IOGitLabUrlProvider extends GitLabUrlProvider[IO]
    val gitLabUrlProvider = mock[IOGitLabUrlProvider]
    val file              = mock[Commands.File]
    val git               = mock[Commands.Git]
    val renku             = mock[Commands.Renku]
    val randomLong        = mockFunction[Long]
    val toRdfTriples      = mockFunction[InputStream, IO[RDFTriples]]
    randomLong.expects().returning(pathDifferentiator)

    val triplesFinder = new IOTriplesFinder(gitLabUrlProvider, file, git, renku, toRdfTriples, randomLong)
  }
}
