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
package projectslug

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.ConditionedMigration
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

class SlugPersisterSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with OptionValues
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with TSTooling {

  it should "persist the given slug" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    provisionProject(project).assertNoException >>
      deleteRenkuSlugProp(project.resourceId).assertNoException >>
      findProjectSlug(project.resourceId).asserting(_ shouldBe None) >>
      findProjectsSlug(project.resourceId).asserting(_ shouldBe None) >>
      persister
        .persistSlug(ProjectInfo(project.resourceId, project.slug))
        .assertNoException >>
      findProjectSlug(project.resourceId).asserting(_.value shouldBe project.slug) >>
      findProjectsSlug(project.resourceId).asserting(_.value shouldBe project.slug) >>
      migrationNeedChecker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.No])
  }

  it should "leave the old dateModified if exists" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    provisionProject(project).assertNoException >>
      findProjectSlug(project.resourceId).asserting(_.value shouldBe project.slug) >>
      findProjectsSlug(project.resourceId).asserting(_.value shouldBe project.slug) >>
      persister
        .persistSlug(ProjectInfo(project.resourceId, project.slug))
        .assertNoException >>
      findProjectSlug(project.resourceId).asserting(_.value shouldBe project.slug) >>
      findProjectsSlug(project.resourceId).asserting(_.value shouldBe project.slug) >>
      migrationNeedChecker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.No])
  }

  implicit override lazy val ioLogger: Logger[IO]                  = TestLogger[IO]()
  private implicit val timeRecorder:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val tsClient             = TSClient[IO](projectsDSConnectionInfo)
  private lazy val persister            = new SlugPersisterImpl[IO](tsClient)
  private lazy val migrationNeedChecker = new MigrationNeedCheckerImpl[IO](tsClient)

  private def findProjectSlug(id: projects.ResourceId): IO[Option[projects.Slug]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test find slug",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?slug
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  GRAPH ?id { ?id renku:slug ?slug }
                 |}
                 |LIMIT 1
                 |""".stripMargin
      )
    ).map(_.map(_.get("slug").map(projects.Slug(_))).headOption.flatten)

  private def findProjectsSlug(id: projects.ResourceId): IO[Option[projects.Slug]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test find slug",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?slug
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  GRAPH ${GraphClass.Projects.id} { ?id renku:slug ?slug }
                 |}
                 |LIMIT 1
                 |""".stripMargin
      )
    ).map(_.map(_.get("slug").map(projects.Slug(_))).headOption.flatten)
}
