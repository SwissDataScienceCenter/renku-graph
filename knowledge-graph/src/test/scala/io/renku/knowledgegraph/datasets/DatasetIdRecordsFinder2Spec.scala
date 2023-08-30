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

package io.renku.knowledgegraph.datasets

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities.Person
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.triplesstore.ProjectSparqlClient

class DatasetIdRecordsFinder2Spec extends SecurityRecordFinderSupport {
  lazy val finder = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(DatasetIdRecordsFinder2.apply[IO])

  it should "find security records for a simple project with one dataset" in {
    val project =
      EntitiesGenerators
        .renkuProjectEntities(
          visibilityGen = EntitiesGenerators.visibilityPrivate,
          creatorGen = EntitiesGenerators.personEntities(EntitiesGenerators.withGitLabId)
        )
        .withDatasets(
          EntitiesGenerators.datasetEntities(EntitiesGenerators.provenanceInternal())
        )
        .suchThat(_.members.nonEmpty)
        .generateOne

    val memberDef: PartialFunction[Person, Role] = {
      case p if p == project.members.head => Role.Owner
    }

    val dsId = project.datasets.head.identification.identifier

    for {
      _ <- provisionTestProjectAndMembers(project, memberDef)
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe List(project).map(toSecRecord)
    } yield ()
  }

  it should "find security records for a forked project" in {
    val (dataset, (parentProject, project)) =
      EntitiesGenerators
        .renkuProjectEntities(EntitiesGenerators.anyVisibility)
        .modify(EntitiesGenerators.removeMembers())
        .addDataset(EntitiesGenerators.datasetEntities(EntitiesGenerators.provenanceNonModified))
        .forkOnce()
        .generateOne

    val dsId = dataset.identification.identifier

    for {
      _ <- provisionTestProjectAndMembers(project)
      _ <- provisionTestProject(parentProject)
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe List(project, parentProject).map(toSecRecord).sortBy(_.projectSlug.value)
    } yield ()
  }
}
