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

package io.renku.entities.viewings.collector.datasets

import cats.effect.IO
import cats.syntax.all._
import io.renku.entities.viewings.collector
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues

class DSInfoFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with OptionValues {

  "findDSInfo" should {

    "return the Project info of the project where the non-modified DS with the given identifier exists" in new TestCase {

      val ds -> project = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      finder.findDSInfo(ds.identification.identifier).unsafeRunSync().value shouldBe
        DSInfo(project.slug, toCollectorDataset(ds))
    }

    "return slug of the project where the modified DS with the given identifier exists" in new TestCase {

      val _ -> modifiedDS -> project = anyRenkuProjectEntities
        .addDatasetAndModification(datasetEntities(provenanceNonModified))
        .generateOne
        .map(_.to[entities.Project])

      upload(to = projectsDataset, project)

      finder.findDSInfo(modifiedDS.identification.identifier).unsafeRunSync().value shouldBe
        DSInfo(project.slug, toCollectorDataset(modifiedDS.to[entities.Dataset[entities.Dataset.Provenance.Modified]]))
    }

    "return slug of the parent project where the DS with the given identifier exists" in new TestCase {

      val parentProject -> project = anyRenkuProjectEntities
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne
        .forkOnce()
        .bimap(
          _.to[entities.Project],
          _.to[entities.RenkuProject.WithParent]
        )

      upload(to = projectsDataset, parentProject, project)

      val ds = project.datasets.headOption.value

      finder.findDSInfo(ds.identification.identifier).unsafeRunSync().value shouldBe
        DSInfo(parentProject.slug, toCollectorDataset(parentProject.datasets.headOption.value))
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new DSInfoFinderImpl[IO](TSClient[IO](projectsDSConnectionInfo))
  }

  private def toCollectorDataset(ds: entities.Dataset[entities.Dataset.Provenance]) =
    collector.persons.Dataset(ds.resourceId, ds.identification.identifier)
}
