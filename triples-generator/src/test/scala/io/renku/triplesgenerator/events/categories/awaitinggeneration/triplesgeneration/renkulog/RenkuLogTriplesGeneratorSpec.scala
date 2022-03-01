/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog

import ammonite.ops.root
import cats.data.EitherT
import cats.data.EitherT._
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.config.ServiceUrl
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, renkuBaseUrls}
import io.renku.graph.model.events.{CommitId, EventId}
import io.renku.graph.model.{RenkuBaseUrl, entities, projects}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, Property}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.{ProcessingNonRecoverableError, ProcessingRecoverableError}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.Commands.{GitLabRepoUrlFinder, RepositoryPath}
import io.renku.triplesgenerator.events.categories.awaitinggeneration.{CommitEvent, categoryName}
import io.renku.triplesgenerator.generators.ErrorGenerators.nonRecoverableMalformedRepoErrors
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import os.Path

class RenkuLogTriplesGeneratorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "generateTriples" should {

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "check if it's a renku repo, " +
      "do not clean the repo if .gitattributes files is committed" +
      "call 'renku migrate', " +
      "call 'renku graph export', " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])

        (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(true.pure[IO])

        (git
          .status(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(sentenceContaining("nothing to commit").generateOne.value.pure[IO])

        (renku
          .migrate(_: CommitEvent)(_: RepositoryPath))
          .expects(commitEvent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .graphExport(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](payload))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync() shouldBe Right(payload)
      }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "check it it's a renku repo, " +
      "clean the repo if it's not clean " +
      "call 'renku migrate', " +
      "call 'renku graph export', " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])

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
          .expects(commitEvent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .graphExport(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](payload))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync() shouldBe Right(payload)
      }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "check if it's a renku repo" +
      "call 'renku migrate', " +
      "call 'renku graph export', " +
      "convert the stream to RDF model and " +
      "removes the temp directory" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])

        (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

        (renku
          .migrate(_: CommitEvent)(_: RepositoryPath))
          .expects(commitEvent, repositoryDirectory)
          .returning(IO.unit)

        (renku
          .graphExport(_: RepositoryPath))
          .expects(repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](payload))

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync() shouldBe Right(payload)
      }

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "check if it's a renku repo" +
      "return the minimal project payload if it's not a renku repo" in new TestCase {

        (file
          .mkdir(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.pure(repositoryDirectory.value))

        (gitLabRepoUrlFinder
          .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(IO.pure(gitRepositoryUrl))

        (git
          .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
          .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
          .returning(rightT[IO, ProcessingRecoverableError](()))

        (git
          .checkout(_: CommitId)(_: RepositoryPath))
          .expects(commitId, repositoryDirectory)
          .returning(IO.unit)

        (file.exists(_: Path)).expects(renkuRepoFilePath).returning(false.pure[IO])

        (file
          .deleteDirectory(_: Path))
          .expects(repositoryDirectory.value)
          .returning(IO.unit)
          .atLeastOnce()

        triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync() shouldBe JsonLD.arr {
          JsonLD
            .entity(
              projects.ResourceId(commitEvent.project.path).asEntityId,
              entities.Project.entityTypes,
              Map.empty[Property, JsonLD]
            )
        }.asRight
      }

    "return LogWorthyRecoverableError if 'renku graph export' returns one" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitEvent, repositoryDirectory)
        .returning(IO.unit)

      val error = LogWorthyRecoverableError(nonBlankStrings().generateOne)
      (renku
        .graphExport(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(leftT(error))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync() shouldBe Left(error)
    }

    "return LogWorthyRecoverableError if cloning the repo returns such error" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      implicit override lazy val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      val exception = LogWorthyRecoverableError(nonBlankStrings().generateOne.value)
      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(EitherT.leftT[IO, Unit](exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync() shouldBe Left(
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
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail finding GitLabUrl fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      val exception = exceptions.generateOne
      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
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
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      val exception = new Exception(s"${exceptions.generateOne.getMessage} $gitRepositoryUrl")
      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(EitherT.liftF[IO, ProcessingRecoverableError, Unit](IO.raiseError(exception)))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitEvent).value.unsafeRunSync()
      }

      actual.getMessage should startWith(s"${commonLogMessage(commitEvent)} triples generation failed: ")
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
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

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
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail when checking if a file exists fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (file
        .exists(_: Path))
        .expects(renkuRepoFilePath)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail when removing a file fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
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
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail when checking out the current revision fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
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
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if calling 'renku migrate' fails with ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      val exception = nonRecoverableMalformedRepoErrors.generateOne
      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitEvent, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[ProcessingNonRecoverableError.MalformedRepository] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} ${exception.message}"
      actual.getCause   shouldBe exception.cause
    }

    "fail if calling 'renku migrate' fails with non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      val exception = exceptions.generateOne
      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitEvent, repositoryDirectory)
        .returning(IO.raiseError(exception))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if calling 'renku graph export' fails with ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitEvent, repositoryDirectory)
        .returning(IO.unit)

      val exception = nonRecoverableMalformedRepoErrors.generateOne
      (renku
        .graphExport(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(EitherT.right[ProcessingRecoverableError](exception.raiseError[IO, JsonLD]))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[ProcessingNonRecoverableError.MalformedRepository] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} ${exception.message}"
      actual.getCause   shouldBe exception.cause
    }

    "fail if calling 'renku graph export' fails with non-ProcessingNonRecoverableError.MalformedRepository" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitEvent, repositoryDirectory)
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (renku
        .graphExport(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(EitherT.right[ProcessingRecoverableError](exception.raiseError[IO, JsonLD]))

      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.unit)
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }

    "fail if removing the temp folder fails" in new TestCase {

      (file
        .mkdir(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.pure(repositoryDirectory.value))

      (gitLabRepoUrlFinder
        .findRepositoryUrl(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(IO.pure(gitRepositoryUrl))

      (git
        .clone(_: ServiceUrl, _: Path)(_: RepositoryPath))
        .expects(gitRepositoryUrl, workDirectory, repositoryDirectory)
        .returning(rightT[IO, ProcessingRecoverableError](()))

      (git
        .checkout(_: CommitId)(_: RepositoryPath))
        .expects(commitId, repositoryDirectory)
        .returning(IO.unit)

      (file.exists(_: Path)).expects(renkuRepoFilePath).returning(true.pure[IO])
      (file.exists(_: Path)).expects(dirtyRepoFilePath).returning(false.pure[IO])

      (renku
        .migrate(_: CommitEvent)(_: RepositoryPath))
        .expects(commitEvent, repositoryDirectory)
        .returning(IO.unit)

      (renku
        .graphExport(_: RepositoryPath))
        .expects(repositoryDirectory)
        .returning(rightT(payload))

      val exception = exceptions.generateOne
      (file
        .deleteDirectory(_: Path))
        .expects(repositoryDirectory.value)
        .returning(IO.raiseError(exception))
        .atLeastOnce()

      val actual = intercept[Exception] {
        triplesGenerator.generateTriples(commitEvent)(maybeAccessToken).value.unsafeRunSync()
      }
      actual.getMessage shouldBe s"${commonLogMessage(commitEvent)} triples generation failed"
      actual.getCause   shouldBe exception
    }
  }

  private trait TestCase {
    implicit lazy val maybeAccessToken: Option[AccessToken] = Gen.option(accessTokens).generateOne
    implicit lazy val renkuBaseUrl:     RenkuBaseUrl        = renkuBaseUrls.generateOne
    lazy val repositoryName = nonEmptyStrings().generateOne
    lazy val projectPath    = projects.Path(s"user/$repositoryName")
    lazy val gitRepositoryUrl = serviceUrls.generateOne / maybeAccessToken
      .map(_.value)
      .getOrElse("path") / s"$projectPath.git"

    lazy val commitEvent @ CommitEvent(_, _, commitId) = {
      val commitId = commitIds.generateOne
      CommitEvent(EventId(commitId.value), Project(projectIds.generateOne, projectPath), commitId)
    }

    val pathDifferentiator: Int = Gen.choose(1, 100).generateOne

    val workDirectory: Path = root / "tmp"
    implicit val repositoryDirectory: RepositoryPath = RepositoryPath(
      workDirectory / s"$repositoryName-$pathDifferentiator"
    )
    val payload:           JsonLD = jsonLDEntities.generateOne
    val dirtyRepoFilePath: Path   = repositoryDirectory.value / ".gitattributes"
    val renkuRepoFilePath: Path   = repositoryDirectory.value / ".renku"

    val gitLabRepoUrlFinder = mock[GitLabRepoUrlFinder[IO]]
    val file                = mock[Commands.File[IO]]
    val git                 = mock[Commands.Git[IO]]
    val renku               = mock[Commands.Renku[IO]]
    val randomLong          = mockFunction[Long]
    randomLong.expects().returning(pathDifferentiator)

    val triplesGenerator = new RenkuLogTriplesGenerator(gitLabRepoUrlFinder, renku, file, git, randomLong)
  }

  private def commonLogMessage(event: CommitEvent): String =
    s"$categoryName: Commit Event ${event.compoundEventId}, ${event.project.path}"
}
