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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.projectsgraph

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder, TSClient}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectFetcherSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with OptionValues {

  it should "fetch info about the project" in {

    val project = anyProjectEntities.suchThat(_.images.size > 1).generateOne.to[entities.Project]

    upload(to = projectsDataset, project)

    fetcher
      .fetchProject(project.path)
      .asserting(
        _.value shouldBe entities.NonRenkuProject.WithoutParent(
          project.resourceId,
          project.path,
          project.name,
          project.maybeDescription,
          project.dateCreated,
          project.dateModified,
          project.maybeCreator.map(p => entities.Person.WithNameOnly(p.resourceId, p.name, None, None)),
          project.visibility,
          project.keywords,
          members = Set.empty,
          project.images
        )
      )
  }

  it should "return no project if one does not exists" in {
    fetcher.fetchProject(projectPaths.generateOne).asserting(_ shouldBe None)
  }

  private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val fetcher = new ProjectFetcherImpl[IO](TSClient[IO](projectsDSConnectionInfo))
}
