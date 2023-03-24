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

package io.renku.entities.search

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.search.Criteria.Filters._
import io.renku.entities.searchgraphs.SearchInfoDataset
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.http.client.AccessToken.UserOAuthAccessToken
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import scala.language.reflectiveCalls

@annotation.nowarn
class LocalSpec
    extends AnyFlatSpec
    with IOSpec
    with InMemoryJenaForSpec
    with ExternalJenaForSpec
    with EntitiesGenerators
    with ProjectsDataset
    with SearchInfoDataset {

  implicit val ioLogger:             TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

  def createProject = {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    upload(to = projectsDataset, project)
  }

  override def clearDatasetsBefore: Boolean = false

  def writeQuery(q: SparqlQuery): Unit = {
    import fs2.io.file._

    val out = Path("/Users/ekettner/org/sdsc/files/q2.sparql")
    // val out = Path("/home/sdsc/org/sdsc/files/q2.sparql")
    fs2.Stream
      .emit(q.toString)
      .through(fs2.text.utf8.encode)
      .through(Files[IO].writeAll(out))
      .compile
      .drain
      .unsafeRunSync()
  }

  it should "play with query" in {
    val x = LocalDate.now()
    val y = EntityType.Dataset
    val criteria =
      Criteria(
        filters = Criteria.Filters(
          maybeQuery = Some(Criteria.Filters.Query("Rok Roskar")),
          entityTypes = Set(EntityType.Dataset)
          // creators = Set("Rok Roskar", "Detlef Sauer")
          // maybeSince = Some(Criteria.Filters.Since(LocalDate.now().minusYears(14))),
          // maybeUntil = Some(Criteria.Filters.Until(LocalDate.now()))
          // namespaces = Set(projects.Namespace("kg.user")),
          // visibilities = Set(projects.Visibility.Private, projects.Visibility.Public)
        ),
        maybeUser = Some(AuthUser(GitLabId(13), UserOAuthAccessToken("bla")))
      )

    // user=13 = Rok, user=88 = me
    val query = DatasetsQuery2.query(criteria).get
    val q = SparqlQuery.of(
      "test",
      Prefixes.of(Schemas.xsd -> "xsd", Schemas.schema -> "schema", Schemas.renku -> "renku", Schemas.text -> "text"),
      query
    )
    println(s"---- query ----\n${q.toString}\n---- ----")
    writeQuery(q)

    val results = queryRunnerFor(projectsDataset).flatMap(_.runQuery(q)).unsafeRunSync()

    println(results)
  }
}
