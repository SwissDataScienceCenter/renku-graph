/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog

import ammonite.ops.{CommandResult, root}
import cats.data.EitherT
import cats.data.EitherT._
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.config.ServiceUrl
import ch.datascience.events.consumers.Project
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.{CommitId, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.{CommitEvent, categoryName}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEvent._
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.Commands.RepositoryPath
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import os.Path

import scala.concurrent.ExecutionContext

class RenkuLogTriplesGeneratorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "generateTriples" should {

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "do not clean the repo if .gitattributes files is committed" +
      "call 'renku migrate', " +
      "call 'renku log' without --revision when no parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, GenerationRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(true.pure[IO])

        (git
          .status(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(sentenceContaining("nothing to commit").generateOne.value.pure[IO])

        (renku
          .migrate(_: CommitEvent)(_: RepositoryPath))
          .expects(commitWithoutParent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .log(_: CommitEventWithoutParent)(_: (CommitEventWithoutParent, Path) => CommandResult, _: RepositoryPath))
          .expects(commitWithoutParent, renku.commitWithoutParentTriplesFinder, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](triples))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitWithoutParent).value.unsafeRunSync() shouldBe Right(triples)
      }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "clean the repo if it's not clean " +
      "call 'renku migrate', " +
      "call 'renku log' without --revision when no parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, GenerationRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(true.pure[IO])

        (git
          .status(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(sentences().generateOne.value.pure[IO])

        (git.rm(_: Path)(_: RepositoryPath)).expects(dirtyRepoFilePath, repositoryDirectory).returning(IO.unit)

        (git
          .`reset --hard`(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(IO.unit)

        (renku
          .migrate(_: CommitEvent)(_: RepositoryPath))
          .expects(commitWithoutParent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .log(_: CommitEventWithoutParent)(_: (CommitEventWithoutParent, Path) => CommandResult, _: RepositoryPath))
          .expects(commitWithoutParent, renku.commitWithoutParentTriplesFinder, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](triples))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitWithoutParent).value.unsafeRunSync() shouldBe Right(triples)
      }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "call 'renku migrate', " +
      "call 'renku log' without --revision when no parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, GenerationRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

        (renku
          .migrate(_: CommitEvent)(_: RepositoryPath))
          .expects(commitWithoutParent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .log(_: CommitEventWithoutParent)(_: (CommitEventWithoutParent, Path) => CommandResult, _: RepositoryPath))
          .expects(commitWithoutParent, renku.commitWithoutParentTriplesFinder, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](triples))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitWithoutParent).value.unsafeRunSync() shouldBe Right(triples)
      }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "call 'renku migrate', " +
      "call 'renku log' with --revision when there's a parent commit, " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, GenerationRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

        val commitWithParent = toCommitWithParent(commitWithoutParent)
        (renku
          .migrate(_: CommitEvent)(_: RepositoryPath))
          .expects(commitWithParent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .log(_: CommitEventWithParent)(_: (CommitEventWithParent, Path) => CommandResult, _: RepositoryPath))
          .expects(commitWithParent, renku.commitWithParentTriplesFinder, repositoryDirectory)
          .returning(rightT(triples))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitWithParent).value.unsafeRunSync() shouldBe Right(triples)
      }

    s"return $GenerationRecoverableError if 'renku log' returns one" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      val commitWithParent = toCommitWithParent(commitWithoutParent)
      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitWithParent, repositoryDirectory)
        .returning(IO.unit)

      val error = GenerationRecoverableError(nonBlankStrings().generateOne)
      (renku
        .log(_: CommitEventWithParent)(_: (CommitEventWithParent, Path) => CommandResult, _: RepositoryPath))
        .expects(commitWithParent, renku.commitWithParentTriplesFinder, repositoryDirectory)
        .returning(leftT(error))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitWithParent).value.unsafeRunSync() shouldBe Left(error)
    }

    s"return $GenerationRecoverableError if cloning the repo returns such error" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      implicit override lazy val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      val exception = GenerationRecoverableError(nonBlankStrings().generateOne.value)
      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(EitherT.leftT[IO, Unit](exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitWithoutParent).value.unsafeRunSync() shouldBe Left(
        exception
      )
    }

    "fail if temp directory creation fails" in new TestCase {

      val exception = exceptions.generateOne
      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.raiseError(exception))

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail finding GitLabUrl fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      val exception = exceptions.generateOne
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if cloning the repo fails - exception message should not reveal the access token" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      val accessToken = accessTokens.generateOne
      implicit override lazy val maybeAccessToken: Option[AccessToken] = Some(accessToken)
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      val exception = new Exception(s"${exceptions.generateOne.getMessage} $gitRepositoryUrl")
      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(EitherT.liftF[IO, GenerationRecoverableError, Unit](IO.raiseError(exception)))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent).value.unsafeRunSync()
      }

      actual.getMessage should startWith(s"${commonLogMessage(commitWithoutParent)} triples generation failed: ")
      actual.getMessage should not include accessToken.value
      actual.getMessage should include(accessToken.toString)
      actual.getCause shouldBe null
    }

    "fail if checking out the sha fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      val exception = exceptions.generateOne
      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail when checking if a file exists" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (file
        .exists(_: Path))
        .expects(dirtyRepoFilePath)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail when removing a file" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(true.pure[IO])

      (git
        .status(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(sentences().generateOne.value.pure[IO])

      val exception = exceptions.generateOne
      (git
        .rm(_: Path)(_: RepositoryPath))
        .expects(dirtyRepoFilePath, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail when checking out the current revision" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(true.pure[IO])

      (git
        .status(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(sentences().generateOne.value.pure[IO])

      (git.rm(_: Path)(_: RepositoryPath)).expects(dirtyRepoFilePath, repositoryDirectory).returning(IO.unit)

      val exception = exceptions.generateOne
      (git
        .`reset --hard`(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if calling 'renku migrate' fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      val exception = exceptions.generateOne
      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitWithoutParent, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if calling 'renku log' fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitWithoutParent, repositoryDirectory)
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (renku
        .log(_: CommitEventWithoutParent)(_: (CommitEventWithoutParent, Path) => CommandResult, _: RepositoryPath))
        .expects(commitWithoutParent, renku.commitWithoutParentTriplesFinder, repositoryDirectory)
        .returning(EitherT.right[ProcessingRecoverableError](exception.raiseError[IO, JsonLDTriples]))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if removing the temp folder fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, GenerationRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitWithoutParent, repositoryDirectory)
        .returning(IO.unit)

      (renku
        .log(_: CommitEventWithoutParent)(_: (CommitEventWithoutParent, Path) => CommandResult, _: RepositoryPath))
        .expects(commitWithoutParent, renku.commitWithoutParentTriplesFinder, repositoryDirectory)
        .returning(rightT(triples))

      val exception = exceptions.generateOne
      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.raiseError(exception))
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitWithoutParent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitWithoutParent)} triples generation failed"
      actual.getCause   shouldBe exception
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {
    implicit lazy val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne
    lazy val repositoryName = nonEmptyStrings().generateOne
    lazy val projectPath    = projects.Path(s"user/$repositoryName")
    lazy val gitRepositoryUrl = serviceUrls.generateOne / maybeAccessToken
      .map(_.value)
      .getOrElse("path") / s"$projectPath.git"

    lazy val commitWithoutParent @ CommitEventWithoutParent(_, _, commitId) = {
      val commitId = commitIds.generateOne
      CommitEventWithoutParent(EventId(commitId.value), Project(projectIds.generateOne, projectPath), commitId)
    }

    def toCommitWithParent(commitWithoutParent: CommitEventWithoutParent): CommitEventWithParent =
      CommitEventWithParent(
        commitWithoutParent.eventId,
        commitWithoutParent.project,
        commitWithoutParent.commitId,
        commitIds.generateOne
      )

    val pathDifferentiator: Int = Gen.choose(1, 100).generateOne

    val workDirectory: Path = root / "tmp"
    implicit val repositoryDirectory: RepositoryPath = RepositoryPath(
      workDirectory / s"$repositoryName-$pathDifferentiator"
    )
    val triples:           JsonLDTriples = jsonLDTriples.generateOne
    val dirtyRepoFilePath: Path          = repositoryDirectory.value / ".gitattributes"

    val gitLabRepoUrlFinder = mock[IOGitLabRepoUrlFinder]
    val file                = mock[Commands.File]
    val git                 = mock[Commands.Git]
    val renku               = mock[Commands.Renku]
    val randomLong          = mockFunction[Long]
    randomLong.expects().returning(pathDifferentiator)

    val triplesGenerator = new RenkuLogTriplesGenerator(gitLabRepoUrlFinder, renku, file, git, randomLong)
  }

  private def commonLogMessage(event: CommitEvent): String =
    s"$categoryName: Commit Event ${event.compoundEventId}, ${event.project.path}"
}
