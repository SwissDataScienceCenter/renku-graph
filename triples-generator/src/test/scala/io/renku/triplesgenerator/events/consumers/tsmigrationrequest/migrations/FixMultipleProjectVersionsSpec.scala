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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.versions.SchemaVersion
import io.renku.graph.model.{GraphClass, entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import io.renku.graph.model.views.TinyTypeToObject._

class FixMultipleProjectVersionsSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with SearchInfoDatasets
    with ProjectsDataset {

  implicit val ioLogger: Logger[IO] = TestLogger()

  it should "remove obsolete project version values when multiple exist" in {

    val currentVersion = SchemaVersion("10")
    val olderVersion   = SchemaVersion("9")
    val project1 =
      anyRenkuProjectEntities
        .map(replaceSchemaVersion(to = currentVersion))
        .generateOne
        .to[entities.RenkuProject]
    val project2 =
      anyRenkuProjectEntities
        .map(replaceSchemaVersion(to = currentVersion))
        .generateOne
        .to[entities.RenkuProject]
    val project3 =
      anyRenkuProjectEntities
        .map(replaceSchemaVersion(to = olderVersion))
        .generateOne
        .to[entities.RenkuProject]

    for {
      _ <- provisionProjects(project1, project2, project3)
      _ <- add(to = project1.resourceId, olderVersion)
      _ <- findProjectVersions.asserting(
             _ shouldBe Map(project1.resourceId -> Set(currentVersion, olderVersion),
                            project2.resourceId -> Set(currentVersion),
                            project3.resourceId -> Set(olderVersion)
             )
           )

      _ <- runUpdate(projectsDataset, FixMultipleProjectVersions.query(currentVersion)).assertNoException

      _ <- findProjectVersions.asserting(
             _ shouldBe Map(project1.resourceId -> Set(currentVersion),
                            project2.resourceId -> Set(currentVersion),
                            project3.resourceId -> Set(olderVersion)
             )
           )
    } yield ()
  }

  private def add(to: projects.ResourceId, version: SchemaVersion) =
    insertIO(
      to = projectsDataset,
      Quad(
        GraphClass.Project.id(to),
        to.asEntityId,
        entities.Project.Ontology.schemaVersionProperty.id,
        version.asObject
      )
    )

  private def findProjectVersions: IO[Map[projects.ResourceId, Set[SchemaVersion]]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "find versions",
        Prefixes of schema -> "schema",
        """|
           |SELECT ?projectId ?version
           |WHERE {
           |  GRAPH ?projectId {
           |    ?projectId a schema:Project;
           |       schema:schemaVersion ?version
           |  }
           |}
           |""".stripMargin
      )
    ).map(
      _.map(row => projects.ResourceId(row("projectId")) -> SchemaVersion(row("version")))
        .groupMap(_._1)(_._2)
        .view
        .mapValues(_.toSet)
        .toMap
    )
}
