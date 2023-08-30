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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities.Person
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl}
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.DatasetProvision
import io.renku.triplesstore.{ExternalJenaForSpec, ProjectsDataset}
import org.scalatest.flatspec.AsyncFlatSpec
import org.typelevel.log4cats.Logger

class DatasetIdRecordsFinder2Spec
    extends AsyncFlatSpec
    with AsyncIOSpec
    // with InMemoryJenaForSpec
    with ExternalJenaForSpec
    with ProjectsDataset
    with DatasetProvision {

  implicit val renkuUrl:  RenkuUrl     = RenkuUrl("http://u.rl")
  implicit val gitlabUrl: GitLabApiUrl = GitLabApiUrl("http://gitl.ab")
  implicit val ioLogger:  Logger[IO]   = TestLogger()

  it should "find security records" in {
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

    provisionTestProjectAndMembers(project, memberDef)

  }
}
