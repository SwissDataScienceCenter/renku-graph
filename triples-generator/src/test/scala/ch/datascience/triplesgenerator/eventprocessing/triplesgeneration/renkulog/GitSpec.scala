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

import ammonite.ops.{Bytes, CommandResult, Path, ShelloutException}
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog.Commands.Git
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class GitSpec extends WordSpec with MockFactory {

  "clone" should {

    "return successful CommandResult when no errors" in new TestCase {

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .returning(CommandResult(exitCode = 0, chunks = Seq.empty))

      git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync() shouldBe Right(())
    }

    ("SSL_ERROR_SYSCALL": NonBlank) +: ("the remote end hung up unexpectedly": NonBlank) +: Nil foreach {
      recoverableError =>
        s"return $GenerationRecoverableError if command fails with a message containing '$recoverableError'" in new TestCase {

          val errorMessage = sentenceContaining(recoverableError).generateOne.value
          val commandResultException = ShelloutException {
            CommandResult(
              exitCode = 1,
              chunks   = Seq(Left(new Bytes(errorMessage.getBytes())))
            )
          }
          cloneCommand
            .expects(repositoryUrl, destDirectory, workDirectory)
            .throwing(commandResultException)

          git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync() shouldBe Left(
            GenerationRecoverableError(
              s"git clone failed with: ${commandResultException.result.toString}"
            )
          )
        }
    }

    "fail if command fails with an unknown message" in new TestCase {

      val commandException = ShelloutException {
        CommandResult(
          exitCode = 1,
          chunks   = Seq(Right(new Bytes(nonBlankStrings().generateOne.value.getBytes())))
        )
      }
      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .throwing(commandException)

      intercept[Exception] {
        git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync()
      }.getMessage shouldBe s"git clone failed with: ${commandException.result.toString}"
    }

    "fail if finding command's message fails" in new TestCase {

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .throwing(ShelloutException {
          CommandResult(
            exitCode = 1,
            chunks   = Nil
          )
        })

      intercept[Exception] {
        git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync()
      } shouldBe an[Exception]
    }
  }

  private trait TestCase {
    val repositoryUrl = serviceUrls.generateOne
    val destDirectory = paths.generateOne
    val workDirectory = paths.generateOne

    val cloneCommand = mockFunction[ServiceUrl, Path, Path, CommandResult]
    val git          = new Git(cloneCommand)
  }
}
