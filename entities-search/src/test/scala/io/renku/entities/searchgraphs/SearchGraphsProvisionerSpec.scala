/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchGraphsProvisionerSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "provisionSearchGraphs" should {

    "provision the Datasets and Projects graphs" in new TestCase {

      givenProjectsGraphProvisioning(project, returning = ().pure[IO])
      givenDatasetsGraphProvisioning(project, returning = ().pure[IO])

      provisioner.provisionSearchGraphs(project).unsafeRunSync() shouldBe ()
    }

    "fail if provisioning Projects graph fails" in new TestCase {

      val exception = exceptions.generateOne
      givenProjectsGraphProvisioning(project, returning = exception.raiseError[IO, Nothing])
      givenDatasetsGraphProvisioning(project, returning = ().pure[IO])

      intercept[Exception](provisioner.provisionSearchGraphs(project).unsafeRunSync()) shouldBe exception
    }

    "fail if provisioning Datasets graph fails" in new TestCase {

      givenProjectsGraphProvisioning(project, returning = ().pure[IO])
      val exception = exceptions.generateOne
      givenDatasetsGraphProvisioning(project, returning = exception.raiseError[IO, Nothing])

      intercept[Exception](provisioner.provisionSearchGraphs(project).unsafeRunSync()) shouldBe exception
    }

    "fail with the Projects provisioning failure if both processes fails" in new TestCase {

      val projectsException = exceptions.generateOne
      givenProjectsGraphProvisioning(project, returning = projectsException.raiseError[IO, Nothing])
      val datasetsException = exceptions.generateOne
      givenDatasetsGraphProvisioning(project, returning = datasetsException.raiseError[IO, Nothing])

      intercept[Exception](provisioner.provisionSearchGraphs(project).unsafeRunSync()) shouldBe projectsException
    }
  }

  private trait TestCase {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    private implicit val logger: TestLogger[IO] = new TestLogger[IO]()
    private val projectsGraphProvisioner = mock[projects.ProjectsGraphProvisioner[IO]]
    private val datasetsGraphProvisioner = mock[datasets.DatasetsGraphProvisioner[IO]]
    val provisioner = new SearchGraphsProvisionerImpl[IO](projectsGraphProvisioner, datasetsGraphProvisioner)

    def givenDatasetsGraphProvisioning(project: entities.Project, returning: IO[Unit]) =
      (datasetsGraphProvisioner.provisionDatasetsGraph _)
        .expects(project)
        .returning(returning)

    def givenProjectsGraphProvisioning(project: entities.Project, returning: IO[Unit]) =
      (projectsGraphProvisioner.provisionProjectsGraph _)
        .expects(project)
        .returning(returning)
  }
}
