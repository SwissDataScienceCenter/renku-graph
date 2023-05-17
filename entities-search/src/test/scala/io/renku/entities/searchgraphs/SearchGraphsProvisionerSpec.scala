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

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class SearchGraphsProvisionerSpec extends AnyWordSpec with should.Matchers with TryValues with MockFactory {

  "provisionSearchGraphs" should {

    "provision the Datasets and Projects graphs" in new TestCase {

      givenProjectsGraphProvisioning(project, returning = ().pure[Try])
      givenDatasetsGraphProvisioning(project, returning = ().pure[Try])

      provisioner.provisionSearchGraphs(project).success.value shouldBe ()
    }

    "fail if provisioning Projects graph fails" in new TestCase {

      val exception = exceptions.generateOne
      givenProjectsGraphProvisioning(project, returning = exception.raiseError[Try, Nothing])
      givenDatasetsGraphProvisioning(project, returning = ().pure[Try])

      provisioner.provisionSearchGraphs(project).failure.exception shouldBe exception
    }

    "fail if provisioning Datasets graph fails" in new TestCase {

      givenProjectsGraphProvisioning(project, returning = ().pure[Try])
      val exception = exceptions.generateOne
      givenDatasetsGraphProvisioning(project, returning = exception.raiseError[Try, Nothing])

      provisioner.provisionSearchGraphs(project).failure.exception shouldBe exception
    }

    "fail with the Projects provisioning failure if both processes fails" in new TestCase {

      val projectsException = exceptions.generateOne
      givenProjectsGraphProvisioning(project, returning = projectsException.raiseError[Try, Nothing])
      val datasetsException = exceptions.generateOne
      givenDatasetsGraphProvisioning(project, returning = datasetsException.raiseError[Try, Nothing])

      provisioner.provisionSearchGraphs(project).failure.exception shouldBe projectsException
    }
  }

  private trait TestCase {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    private implicit val logger: TestLogger[Try] = new TestLogger[Try]()
    private val projectsGraphProvisioner = mock[projects.ProjectsGraphProvisioner[Try]]
    private val datasetsGraphProvisioner = mock[datasets.DatasetsGraphProvisioner[Try]]
    val provisioner = new SearchGraphsProvisionerImpl[Try](projectsGraphProvisioner, datasetsGraphProvisioner)

    def givenDatasetsGraphProvisioning(project: entities.Project, returning: Try[Unit]) =
      (datasetsGraphProvisioner.provisionDatasetsGraph _)
        .expects(project)
        .returning(returning)

    def givenProjectsGraphProvisioning(project: entities.Project, returning: Try[Unit]) =
      (projectsGraphProvisioner.provisionProjectsGraph _)
        .expects(project)
        .returning(returning)
  }
}
