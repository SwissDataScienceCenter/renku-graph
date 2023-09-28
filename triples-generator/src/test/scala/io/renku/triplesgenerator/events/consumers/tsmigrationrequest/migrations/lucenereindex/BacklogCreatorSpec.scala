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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package lucenereindex

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.testentities._
import io.renku.graph.model.versions.SchemaVersion
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling.AllProjects

class BacklogCreatorSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MigrationsDataset
    with MockFactory {

  private val pageSize = 50

  "createBacklog" should {

    "find all projects that are in the TS" in new TestCase {

      val projects = anyRenkuProjectEntities
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
        .map(_.to[entities.Project])

      fetchBacklogProjects shouldBe Nil

      upload(to = projectsDataset, projects: _*)(implicitly[EntityFunctions[entities.Project]],
                                                 projectsDSGraphsProducer[entities.Project],
                                                 ioRuntime
      )

      backlogCreator.createBacklog().unsafeRunSync() shouldBe ()

      fetchBacklogProjects.toSet shouldBe projects.map(_.slug).toSet
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val backlogCreator =
      new BacklogCreatorImpl[IO](AllProjects[IO](projectsDSConnectionInfo), TSClient[IO](migrationsDSConnectionInfo))

    def fetchBacklogProjects =
      runSelect(
        on = migrationsDataset,
        SparqlQuery.ofUnsafe(
          "test ReindexLucene backlog",
          Prefixes of renku -> "renku",
          sparql"""|SELECT ?slug
                   |WHERE {
                   |  ${ReindexLucene.name.asEntityId} renku:toBeMigrated ?slug
                   |}
                   |""".stripMargin
        )
      ).unsafeRunSync().flatMap(_.get("slug").map(projects.Slug))
  }

  private def setSchema(version: SchemaVersion): Project => Project =
    _.fold(_.copy(version = version), _.copy(version = version), identity, identity)
}
