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

package io.renku.entities.searchgraphs
package projects

import cats.syntax.all._
import commands.UpdateCommandsProducer
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectsGraphProvisionerSpec extends AnyWordSpec with should.Matchers with TryValues with MockFactory {

  "provisionProjectsGraph" should {

    "extract the Project graph relevant data" +
      "produce TS update commands and " +
      "push the commands into the TS" in new TestCase {

        val project = anyProjectEntities.generateOne.to[entities.Project]

        val searchInfo = SearchInfoExtractor.extractSearchInfo(project)

        val updates = updateCommands.generateList()
        givenUpdatesProducing(searchInfo, returning = updates)

        givenUploading(updates, returning = ().pure[Try])

        provisioner.provisionProjectsGraph(project).success.value shouldBe ()
      }

    "fail if updates uploading fails" in new TestCase {

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      val searchInfo = SearchInfoExtractor.extractSearchInfo(project)

      val updates = updateCommands.generateList()
      givenUpdatesProducing(searchInfo, returning = updates)

      val failure = exceptions.generateOne.raiseError[Try, Unit]
      givenUploading(updates, returning = failure)

      provisioner.provisionProjectsGraph(project) shouldBe failure
    }
  }

  private trait TestCase {

    private val commandsProducer = mock[UpdateCommandsProducer]
    private val commandsUploader = mock[UpdateCommandsUploader[Try]]
    val provisioner              = new ProjectsGraphProvisionerImpl[Try](commandsProducer, commandsUploader)

    def givenUpdatesProducing(searchInfo: ProjectSearchInfo, returning: List[UpdateCommand]) =
      (commandsProducer.toUpdateCommands _)
        .expects(searchInfo)
        .returning(returning)

    def givenUploading(commands: List[UpdateCommand], returning: Try[Unit]) =
      (commandsUploader.upload _)
        .expects(commands)
        .returning(returning)
  }
}
