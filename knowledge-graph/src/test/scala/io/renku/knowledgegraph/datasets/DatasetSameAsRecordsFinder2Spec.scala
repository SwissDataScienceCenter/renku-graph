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
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities.Person
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl}
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.DatasetProvision
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectSparqlClient, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

class DatasetSameAsRecordsFinder2Spec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    // with ExternalJenaForSpec
    with ProjectsDataset
    with DatasetProvision {

  implicit val renkuUrl:  RenkuUrl                    = RenkuUrl("http://u.rl")
  implicit val gitlabUrl: GitLabApiUrl                = GitLabApiUrl("http://gitl.ab")
  implicit val ioLogger:  Logger[IO]                  = TestLogger()
  implicit val sqtr:      SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe

  lazy val finder = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(DatasetSameAsRecordsFinder2.apply[IO])

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

    val dsSameAs = SameAs(project.datasets.head.provenance.topmostSameAs.value)

    for {
      _ <- provisionTestProjectAndMembers(project, memberDef)
      r <- finder.use(_.apply(dsSameAs, None))
      _ = r.size                shouldBe 1
      _ = r.head.projectSlug    shouldBe project.slug
      _ = r.head.visibility     shouldBe project.visibility
      _ = r.head.allowedPersons shouldBe project.members.flatMap(_.maybeGitLabId)
    } yield ()
  }
}
