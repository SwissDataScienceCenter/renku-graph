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
import io.renku.entities.searchgraphs.datasets.DatasetsGraphCleaner
import io.renku.entities.searchgraphs.projects.ProjectsGraphCleaner
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.testentities.projectIdentifications
import io.renku.interpreters.TestLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class SearchGraphsCleanerSpec extends AnyFlatSpec with should.Matchers with MockFactory with TryValues {

  it should "clean Projects and Datasets graphs" in new TestCase {

    givenProjectsGraphProvisioning(projectIdentification, returning = ().pure[Try])
    givenDatasetsGraphProvisioning(projectIdentification, returning = ().pure[Try])

    cleaner.cleanSearchGraphs(projectIdentification).success.value shouldBe ()
  }

  it should "fail if cleaning Datasets graph fails" in new TestCase {

    givenProjectsGraphProvisioning(projectIdentification, returning = ().pure[Try])
    val exception = exceptions.generateOne
    givenDatasetsGraphProvisioning(projectIdentification, returning = exception.raiseError[Try, Nothing])

    cleaner.cleanSearchGraphs(projectIdentification).failure.exception shouldBe exception
  }

  it should "fail with the Projects cleaning failure if both processes fails" in new TestCase {

    val projectsException = exceptions.generateOne
    givenProjectsGraphProvisioning(projectIdentification, returning = projectsException.raiseError[Try, Nothing])
    val datasetsException = exceptions.generateOne
    givenDatasetsGraphProvisioning(projectIdentification, returning = datasetsException.raiseError[Try, Nothing])

    cleaner.cleanSearchGraphs(projectIdentification).failure.exception shouldBe projectsException
  }

  private trait TestCase {

    val projectIdentification = projectIdentifications.generateOne

    private lazy val projectsGraphCleaner = mock[ProjectsGraphCleaner[Try]]
    private lazy val datasetsGraphCleaner = mock[DatasetsGraphCleaner[Try]]
    private implicit val logger: TestLogger[Try] = new TestLogger[Try]()
    val cleaner = new SearchGraphsCleanerImpl(projectsGraphCleaner, datasetsGraphCleaner)

    def givenProjectsGraphProvisioning(projectIdentification: ProjectIdentification, returning: Try[Unit]) =
      (projectsGraphCleaner.cleanProjectsGraph _)
        .expects(projectIdentification)
        .returning(returning)

    def givenDatasetsGraphProvisioning(projectIdentification: ProjectIdentification, returning: Try[Unit]) =
      (datasetsGraphCleaner.cleanDatasetsGraph _)
        .expects(projectIdentification)
        .returning(returning)
  }
}
