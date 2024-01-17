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

package io.renku.knowledgegraph.datasets

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{RenkuUrl, testentities}
import io.renku.http.client.GitLabApiUrl
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.DatasetProvision
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.{GraphJenaSpec, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

abstract class SecurityRecordFinderSupport
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers
    with DatasetProvision {

  implicit val renkuUrl:  RenkuUrl                    = RenkuUrl("http://u.rl")
  implicit val gitlabUrl: GitLabApiUrl                = GitLabApiUrl("http://gitl.ab")
  implicit val ioLogger:  Logger[IO]                  = TestLogger()
  implicit val sqtr:      SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe

  def toSecRecord(p: testentities.Project) =
    Authorizer.SecurityRecord(p.visibility, p.slug, p.members.flatMap(_.person.maybeGitLabId))

  def projectWithDatasetAndMembers =
    EntitiesGenerators
      .renkuProjectEntities(
        visibilityGen = EntitiesGenerators.anyVisibility,
        creatorGen = EntitiesGenerators.personEntities(EntitiesGenerators.withGitLabId)
      )
      .withDatasets(
        EntitiesGenerators.datasetEntities(EntitiesGenerators.provenanceInternal())
      )
      .suchThat(_.members.nonEmpty)

  def projectWithDatasetAndNoMembers =
    EntitiesGenerators
      .renkuProjectEntities(
        visibilityGen = EntitiesGenerators.anyVisibility,
        creatorGen = EntitiesGenerators.personEntities(EntitiesGenerators.withGitLabId)
      )
      .modify(EntitiesGenerators.removeMembers())
      .withDatasets(
        EntitiesGenerators.datasetEntities(EntitiesGenerators.provenanceInternal())
      )

  def projectAndFork =
    EntitiesGenerators
      .renkuProjectEntities(EntitiesGenerators.anyVisibility)
      .addDataset(EntitiesGenerators.datasetEntities(EntitiesGenerators.provenanceNonModified))
      .forkOnce()
}
