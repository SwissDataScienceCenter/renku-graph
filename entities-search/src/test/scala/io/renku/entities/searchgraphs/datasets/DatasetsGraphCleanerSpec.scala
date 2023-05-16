/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.entities.searchgraphs.datasets

import cats.syntax.all._
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommandsProducer
import io.renku.entities.searchgraphs.{UpdateCommand, UpdateCommandsUploader}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities
import io.renku.graph.model.testentities.projectIdentifications
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class DatasetsGraphCleanerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "cleanDatasetsGraph" should {

    "produce TS update commands and " +
      "push the commands into the TS" in new TestCase {

        val project = projectIdentifications.generateOne

        val updates = updateCommands.generateList()
        givenUpdatesProducing(project, returning = updates.pure[Try])

        givenUploading(updates, returning = ().pure[Try])

        cleaner.cleanDatasetsGraph(project) shouldBe ().pure[Try]
      }

    "fail if updates producing step fails" in new TestCase {

      val project = projectIdentifications.generateOne

      val failure = exceptions.generateOne.raiseError[Try, List[UpdateCommand]]
      givenUpdatesProducing(project, returning = failure)

      cleaner.cleanDatasetsGraph(project) shouldBe failure
    }

    "fail if updates uploading fails" in new TestCase {

      val project = projectIdentifications.generateOne

      val updates = updateCommands.generateList()
      givenUpdatesProducing(project, returning = updates.pure[Try])

      val failure = exceptions.generateOne.raiseError[Try, Unit]
      givenUploading(updates, returning = failure)

      cleaner.cleanDatasetsGraph(project) shouldBe failure
    }
  }

  private trait TestCase {

    private val commandsProducer = mock[UpdateCommandsProducer[Try]]
    private val commandsUploader = mock[UpdateCommandsUploader[Try]]
    val cleaner                  = new DatasetsGraphCleanerImpl[Try](commandsProducer, commandsUploader)

    def givenUpdatesProducing(project: entities.ProjectIdentification, returning: Try[List[UpdateCommand]]) =
      (commandsProducer
        .toUpdateCommands(_: entities.ProjectIdentification)(_: List[DatasetSearchInfo]))
        .expects(project, Nil)
        .returning(returning)

    def givenUploading(commands: List[UpdateCommand], returning: Try[Unit]) =
      (commandsUploader.upload _)
        .expects(commands)
        .returning(returning)
  }
}
