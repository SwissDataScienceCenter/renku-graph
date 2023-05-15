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

package io.renku.entities.searchgraphs.projects

import cats.syntax.all._
import io.renku.entities.searchgraphs.projects.commands.ProjectInfoDeleteQuery
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.projectIdentifications
import io.renku.triplesstore.TSClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class ProjectsGraphCleanerSpec extends AnyFlatSpec with should.Matchers with MockFactory with TryValues {

  private val tsClient     = mock[TSClient[Try]]
  private val graphCleaner = new ProjectsGraphCleanerImpl[Try](tsClient)

  it should "execute the Projects Graph Cleaner query for the given project on the TS" in {

    val projectId = projectIdentifications.generateOne

    (tsClient.updateWithNoResult _)
      .expects(ProjectInfoDeleteQuery(projectId.resourceId))
      .returning(().pure[Try])

    graphCleaner.cleanProjectsGraph(projectId).success.value shouldBe ()
  }
}
