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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.datemodified

import cats.effect.IO
import io.renku.entities.searchgraphs.SearchInfoDatasets
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
import org.typelevel.log4cats.Logger

class ProjectFetcherSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with OptionValues {

  it should "fetch info about the project" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    provisionProject(project).assertNoException >>
      fetcher
        .fetchProject(project.slug)
        .asserting(_.value shouldBe ProjectInfo(project.resourceId, project.slug, project.dateCreated))
  }

  it should "return no project if one does not exists" in {
    fetcher.fetchProject(projectSlugs.generateOne).asserting(_ shouldBe None)
  }

  implicit override lazy val ioLogger: Logger[IO]                  = TestLogger[IO]()
  private implicit val timeRecorder:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val fetcher = new ProjectFetcherImpl[IO](TSClient[IO](projectsDSConnectionInfo))
}
