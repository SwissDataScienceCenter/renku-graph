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

import java.nio.file.Paths

import ammonite.ops.{Bytes, CommandResult, Path}
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.triplesgenerator.eventprocessing.Commands.Renku
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class RenkuSpec extends WordSpec {

  "log" should {

    "return the 'renku log' output if it executes quicker than the defined timeout" in new TestCase {
      val commandBody   = nonEmptyStrings().generateOne
      val commandResult = CommandResult(exitCode = 0, chunks = Seq(Left(new Bytes(commandBody.getBytes()))))

      val result = renku.log(commit, path)(triplesGeneration(returning = commandResult)).unsafeRunSync()

      Source.fromInputStream(result).mkString shouldBe commandBody
    }

    "fail if calling 'renku log' results in a failure" in new TestCase {
      val exception = exceptions.generateOne

      intercept[Exception] {
        renku.log(commit, path)(triplesGeneration(failingWith = exception)).unsafeRunSync()
      } shouldBe exception
    }

    "get terminated if calling 'renku log' takes longer than the defined timeout" in new TestCase {
      intercept[Exception] {
        renku.log(commit, path)(triplesGenerationTakingTooLong).unsafeRunSync()
      }.getMessage shouldBe s"'renku log' execution for commit: ${commit.id}, project: ${commit.project.id} " +
        s"took longer than $renkuLogTimeout - terminating"
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {
    val commit = commits.generateOne
    val path   = paths.generateOne

    val renkuLogTimeout = 1500 millis
    val renku           = new Renku(renkuLogTimeout)

    def triplesGeneration(returning: CommandResult): (Commit, Path) => CommandResult =
      (_, _) => {
        Thread.sleep((renkuLogTimeout - (200 millis)).toMillis)
        returning
      }

    def triplesGeneration(failingWith: Exception): (Commit, Path) => CommandResult =
      (_, _) => throw failingWith

    val triplesGenerationTakingTooLong: (Commit, Path) => CommandResult =
      (_, _) => {
        Thread.sleep((renkuLogTimeout + (5 seconds)).toMillis)
        CommandResult(exitCode = 0, chunks = Nil)
      }
  }

  private val commits: Gen[Commit] = for {
    commitId      <- commitIds
    project       <- projects
    maybeParentId <- Gen.option(commitIds)
  } yield
    maybeParentId match {
      case None           => CommitWithoutParent(commitId, project)
      case Some(parentId) => CommitWithParent(commitId, parentId, project)
    }

  private val paths: Gen[os.Path] = for {
    path <- relativePaths()
  } yield os.Path(Paths.get(s"/$path"))
}
