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

package ch.datascience.webhookservice.eventprocessing.commitevent

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{AccessDeniedException, Files, Path}

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class FileCommitEventStorageSpec extends WordSpec with PropertyChecks {

  "append" should {

    "append all given lines to an empty file" in new TestCase {

      val linesToAppend: List[String] = lines.generateOne

      linesToAppend foreach { line =>
        fileStorage.append(line) shouldBe Success(())
      }

      Files.readAllLines(pathToEventLogFile).asScala.toList shouldBe linesToAppend
    }

    "append given lines to a non-empty file" in new TestCase {

      val initialLines = Seq(nonEmptyStrings().generateOne)
      val fileWriter   = Files.write(pathToEventLogFile, initialLines.asJava)
      Files.readAllLines(pathToEventLogFile).asScala.toList shouldBe initialLines

      val linesToAppend: List[String] = lines.generateOne

      linesToAppend foreach { line =>
        fileStorage.append(line) shouldBe Success(())
      }

      Files.readAllLines(pathToEventLogFile).asScala.toList shouldBe initialLines ++ linesToAppend
    }

    "fail if there are problems with writing to the file" in new TestCase {

      Files.setPosixFilePermissions(pathToEventLogFile, PosixFilePermissions.fromString("r--r--r--"))

      val Failure(exception) = fileStorage.append("line")

      exception shouldBe an[AccessDeniedException]
    }
  }

  private trait TestCase {
    val pathToEventLogFile: Path = Files.createTempFile("renku-event", "log")
    val fileStorage = new FileCommitEventStorage[Try](pathToEventLogFile)
  }

  private val lines: Gen[List[String]] = for {
    linesNumber <- positiveInts(100)
    lines       <- Gen.listOfN(linesNumber, nonEmptyStrings(maxLength = 100))
  } yield lines
}
