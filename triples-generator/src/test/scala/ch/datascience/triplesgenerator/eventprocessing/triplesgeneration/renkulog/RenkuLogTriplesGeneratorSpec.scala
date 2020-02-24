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

package ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog

import java.io.InputStream

import ammonite.ops.{CommandResult, root}
import cats.data.EitherT
import cats.data.EitherT.rightT
import cats.effect.{ContextShift, IO}
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.{CommitId, Project}
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import os.Path

import scala.concurrent.ExecutionContext

class RenkuLogTriplesGeneratorSpec extends WordSpec with MockFactory {

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

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(rightT[IO, GenerationRecoverableError](successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(IO.pure(triples))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync() shouldBe Right(
        triples
      )
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

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(rightT[IO, GenerationRecoverableError](successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      val commitWithParent = toCommitWithParent(commitWithoutParent)
      (renku
        .log(_: CommitWithParent, _: Path)(_: (CommitWithParent, Path) => CommandResult))
        .expects(commitWithParent, repositoryDirectory, renku.commitWithParentTriplesFinder)
        .returning(IO.pure(triples))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitWithParent, maybeAccessToken).value.unsafeRunSync() shouldBe Right(triples)
    }

    s"return $GenerationRecoverableError if cloning the repo returns such error" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      val accessToken                    = accessTokens.generateOne
      override lazy val maybeAccessToken = Some(accessToken)
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      val exception = GenerationRecoverableError(nonBlankStrings().generateOne.value)
      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(EitherT.leftT[IO, CommandResult](exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync() shouldBe Left(
        exception
      )
    }

    "fail if temp directory creation fails" in new TestCase {

      val exception = exceptions.generateOne
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.raiseError(exception))

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync()
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
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if cloning the repo fails - exception message should not reveal the access token" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      val accessToken                    = accessTokens.generateOne
      override lazy val maybeAccessToken = Some(accessToken)
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      val exception = new Exception(s"${exceptions.generateOne.getMessage} $gitRepositoryUrl")
      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(EitherT.liftF[IO, GenerationRecoverableError, CommandResult](IO.raiseError(exception)))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync()
      }

      actual.getMessage should startWith("Triples generation failed: ")
      actual.getMessage should not include accessToken.value
      actual.getMessage should include(accessToken.toString)
      actual.getCause   shouldBe null
    }

    "fail if checking out the sha fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(rightT[IO, GenerationRecoverableError](successfulCommandResult))

      val exception = exceptions.generateOne
      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if calling 'renku log' fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(rightT[IO, GenerationRecoverableError](successfulCommandResult))

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
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if removing the temp folder fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.pure(repositoryDirectory))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: ProjectPath, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path, _: Path))
        .expects(gitRepositoryUrl, repositoryDirectory, workDirectory)
        .returning(rightT[IO, GenerationRecoverableError](successfulCommandResult))

      (git
        .checkout(_: CommitId, _: Path))
        .expects(commitId, repositoryDirectory)
        .returning(IO.pure(successfulCommandResult))

      (renku
        .log(_: CommitWithoutParent, _: Path)(_: (CommitWithoutParent, Path) => CommandResult))
        .expects(commitWithoutParent, repositoryDirectory, renku.commitWithoutParentTriplesFinder)
        .returning(IO.pure(triples))

      val exception = exceptions.generateOne
      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory)
        .returning(IO.raiseError(exception))
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent, maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe "Triples generation failed"
      actual.getCause   shouldBe exception
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {
    val successfulCommandResult = CommandResult(exitCode = 0, chunks = Nil)

    lazy val repositoryName   = nonEmptyStrings().generateOne
    lazy val projectPath      = ProjectPath(s"user/$repositoryName")
    lazy val maybeAccessToken = Gen.option(accessTokens).generateOne
    lazy val gitRepositoryUrl = serviceUrls.generateOne / maybeAccessToken
      .map(_.value)
      .getOrElse("path") / s"$projectPath.git"
    lazy val commitWithoutParent @ CommitWithoutParent(commitId, _) =
      CommitWithoutParent(commitIds.generateOne, Project(projectIds.generateOne, projectPath))

    def toCommitWithParent(commitWithoutParent: CommitWithoutParent): CommitWithParent =
      CommitWithParent(
        commitWithoutParent.id,
        commitIds.generateOne,
        commitWithoutParent.project
      )

    val pathDifferentiator: Int = Gen.choose(1, 100).generateOne

    val workDirectory:       Path          = root / "tmp"
    val repositoryDirectory: Path          = workDirectory / s"$repositoryName-$pathDifferentiator"
    val rdfTriplesStream:    InputStream   = mock[InputStream]
    val triples:             JsonLDTriples = jsonLDTriples.generateOne

    val gitLabRepoUrlFinder = mock[IOGitLabRepoUrlFinder]
    val file                = mock[Commands.File]
    val git                 = mock[Commands.Git]
    val renku               = mock[Commands.Renku]
    val randomLong          = mockFunction[Long]
    randomLong.expects().returning(pathDifferentiator)

    val triplesGenerator = new RenkuLogTriplesGenerator(gitLabRepoUrlFinder, renku, file, git, randomLong)
  }
}
