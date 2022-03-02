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

import ammonite.ops.{Bytes, CommandResult, Path, ShelloutException}
import cats.effect.IO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.config.ServiceUrl
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.ProcessingNonRecoverableError
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.Commands.{GitImpl, RepositoryPath}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "clone" should {

    "return successful CommandResult when no errors" in new TestCase {

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .returning(CommandResult(exitCode = 0, chunks = Seq.empty))

      git.clone(repositoryUrl, workDirectory).value.unsafeRunSync() shouldBe Right(())
    }

    "return AuthRecoverableError if command fails with a message 'remote: HTTP Basic: Access denied'" in new TestCase {

      val errorMessage = sentenceContaining(
        Refined.unsafeApply("""|remote: HTTP Basic: Access denied
                               |fatal: Authentication failed for""".stripMargin)
      ).generateOne.value
      val commandResultException = ShelloutException {
        CommandResult(
          exitCode = 1,
          chunks = Seq(Left(new Bytes(errorMessage.getBytes())))
        )
      }
      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .throwing(commandResultException)

      git.clone(repositoryUrl, workDirectory).value.unsafeRunSync() shouldBe Left(
        AuthRecoverableError(s"git clone failed with: ${commandResultException.result.toString}")
      )
    }

    val recoverableFailures = Set[NonBlank](
      "SSL_ERROR_SYSCALL",
      "The requested URL returned error: 502",
      "The requested URL returned error: 503",
      "Could not resolve host: renkulab.io",
      "Failed to connect to renkulab.io port 443: Host is unreachable"
    )
    recoverableFailures foreach { error =>
      s"return LogWorthyRecoverableError if command fails with a message containing '$error'" in new TestCase {

        val errorMessage = sentenceContaining(error).generateOne.value
        val commandResultException = ShelloutException {
          CommandResult(
            exitCode = 1,
            chunks = Seq(Left(new Bytes(errorMessage.getBytes())))
          )
        }
        cloneCommand
          .expects(repositoryUrl, destDirectory, workDirectory)
          .throwing(commandResultException)

        git.clone(repositoryUrl, workDirectory).value.unsafeRunSync() shouldBe Left(
          LogWorthyRecoverableError(
            s"git clone failed with: ${commandResultException.result.toString}"
          )
        )
      }
    }

    val malformedRepoFailures = Set[NonBlank](
      "fatal: the remote end hung up unexpectedly",
      "The requested URL returned error: 504",
      "Error in the HTTP2 framing layer"
    )
    malformedRepoFailures foreach { error =>
      s"return ProcessingNonRecoverableError.MalformedRepository if command fails with a message containing '$error'" in new TestCase {

        val errorMessage = sentenceContaining(error).generateOne.value
        val commandResultException = ShelloutException {
          CommandResult(
            exitCode = 1,
            chunks = Seq(Left(new Bytes(errorMessage.getBytes())))
          )
        }
        cloneCommand
          .expects(repositoryUrl, destDirectory, workDirectory)
          .throwing(commandResultException)

        val failure = intercept[ProcessingNonRecoverableError.MalformedRepository] {
          git.clone(repositoryUrl, workDirectory).value.unsafeRunSync()
        }

        failure.getMessage shouldBe s"git clone failed with: ${commandResultException.result.toString}"
      }
    }

    "fail if command fails with an unknown message" in new TestCase {

      val commandException = ShelloutException {
        CommandResult(
          exitCode = 1,
          chunks = Seq(Right(new Bytes(nonBlankStrings().generateOne.value.getBytes())))
        )
      }
      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .throwing(commandException)

      intercept[Exception] {
        git.clone(repositoryUrl, workDirectory).value.unsafeRunSync()
      }.getMessage shouldBe s"git clone failed with: ${commandException.result.toString}"
    }

    "fail if finding command's message fails" in new TestCase {

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .throwing(ShelloutException {
          CommandResult(
            exitCode = 1,
            chunks = Nil
          )
        })

      intercept[Exception] {
        git.clone(repositoryUrl, workDirectory).value.unsafeRunSync()
      } shouldBe an[Exception]
    }
  }

  private trait TestCase {
    val repositoryUrl = serviceUrls.generateOne
    implicit val destDirectory: RepositoryPath = RepositoryPath(paths.generateOne)
    val workDirectory = paths.generateOne

    val cloneCommand = mockFunction[ServiceUrl, RepositoryPath, Path, CommandResult]
    val git          = new GitImpl[IO](cloneCommand)
  }
}
