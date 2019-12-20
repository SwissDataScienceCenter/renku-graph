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

package ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog

import ammonite.ops.{Bytes, CommandResult, Path}
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

      val commandResult = CommandResult(exitCode = 0, chunks = Seq.empty)

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .returning(commandResult)

      git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync() shouldBe Right(commandResult)
    }

    s"$GenerationRecoverableError if command fails with a message containing SSL_ERROR_SYSCALL" in new TestCase {

      val errorMessage = sentenceContaining("SSL_ERROR_SYSCALL").generateOne.value
      val commandResult = CommandResult(
        exitCode = 1,
        chunks   = Seq(Left(new Bytes(errorMessage.getBytes())))
      )

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .returning(commandResult)

      git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync() shouldBe Left(
        GenerationRecoverableError(errorMessage)
      )
    }

    "fail if command fails with a message not containing SSL_ERROR_SYSCALL" in new TestCase {

      val errorMessage = nonBlankStrings().generateOne.value
      val commandResult = CommandResult(
        exitCode = 1,
        chunks   = Seq(Left(new Bytes(errorMessage.getBytes())))
      )

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .returning(commandResult)

      intercept[Exception] {
        git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync()
      }.getMessage shouldBe errorMessage
    }

    "fail if executing the command fails" in new TestCase {

      val exception = exceptions.generateOne
      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .throwing(exception)

      intercept[Exception] {
        git.clone(repositoryUrl, destDirectory, workDirectory).value.unsafeRunSync()
      } shouldBe exception
    }

    "fail if finding command's message fails" in new TestCase {

      val commandResult = CommandResult(
        exitCode = 1,
        chunks   = Nil
      )

      cloneCommand
        .expects(repositoryUrl, destDirectory, workDirectory)
        .returning(commandResult)

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
