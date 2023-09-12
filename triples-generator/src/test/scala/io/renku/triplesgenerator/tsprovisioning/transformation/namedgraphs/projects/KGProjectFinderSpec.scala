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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.projects

import TestDataTools._
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with EntitiesGenerators
    with AdditionalMatchers
    with MoreDiffInstances
    with ScalaCheckPropertyChecks
    with InMemoryJenaForSpec
    with ProjectsDataset {

  forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
    it should show"return project's mutable properties for a given ResourceId - project ${project.name}" in {
      upload(to = projectsDataset, project)

      val expected = toProjectMutableData(project).some
      finder
        .find(project.resourceId)
        .asserting(_.map(_.selectEarliestDateCreated) shouldMatchTo expected)
    }
  }

  forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
    it should show"return no keywords if there are none for the given project - project ${project.name}" in {
      val projectNoKeywords = project match {
        case p: entities.RenkuProject.WithParent       => p.copy(keywords = Set.empty)
        case p: entities.RenkuProject.WithoutParent    => p.copy(keywords = Set.empty)
        case p: entities.NonRenkuProject.WithParent    => p.copy(keywords = Set.empty)
        case p: entities.NonRenkuProject.WithoutParent => p.copy(keywords = Set.empty)
      }

      upload(to = projectsDataset, projectNoKeywords)

      finder
        .find(project.resourceId)
        .asserting(_ shouldMatchTo toProjectMutableData(project).copy(keywords = Set.empty).some)
    }
  }

  it should "return no None if there's no Project with the given resourceId" in {
    finder.find(projectResourceIds.generateOne).asserting(_ shouldBe None)
  }

  private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val finder = new KGProjectFinderImpl[IO](projectsDSConnectionInfo)
}
