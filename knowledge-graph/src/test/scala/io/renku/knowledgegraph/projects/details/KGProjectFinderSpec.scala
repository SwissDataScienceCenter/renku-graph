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

package io.renku.knowledgegraph.projects.details

import Converters._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.countingGen
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities.Project
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.KnowledgeGraphJenaSpec
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.log4cats.Logger

class KGProjectFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with KnowledgeGraphJenaSpec
    with TestSearchInfoDatasets
    with EntitiesGenerators
    with ScalaCheckPropertyChecks
    with should.Matchers {

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()

  "findProject" should {

    forAll(Gen.oneOf(renkuProjectEntities(anyVisibility), nonRenkuProjectEntities(anyVisibility)), countingGen) {
      case (project, attempt) =>
        s"return details of the project with the given slug when there's no parent - #$attempt" in projectsDSConfig
          .use { implicit pcc =>
            provisionTestProjects(anyProjectEntities.generateOne, project) >>
              finder
                .findProject(project.slug, authUsers.generateOption)
                .asserting(_ shouldBe project.to(kgProjectConverter).some)
          }
    }

    forAll(
      Gen.oneOf(renkuProjectWithParentEntities(visibilityPublic), nonRenkuProjectWithParentEntities(visibilityPublic)),
      countingGen
    ) { (project, attempt) =>
      s"return details of the project with the given slug if it has a parent project - public projects - #$attempt" in projectsDSConfig
        .use { implicit pcc =>
          provisionTestProjects(project, project.parent) >>
            finder
              .findProject(project.slug, authUsers.generateOption)
              .asserting(_ shouldBe project.to(kgProjectConverter).some)
        }
    }

    "return details of the project with the given slug if it has a parent project - non-public projects" in projectsDSConfig
      .use { implicit pcc =>
        val user         = authUsers.generateOne
        val userAsMember = projectMemberEntities(Gen.const(user.id.some)).generateOne

        val project = Gen
          .oneOf(
            renkuProjectWithParentEntities(visibilityNonPublic).modify(replaceMembers(Set(userAsMember))),
            nonRenkuProjectWithParentEntities(visibilityNonPublic).modify(replaceMembers(Set(userAsMember)))
          )
          .generateOne

        val parent = replaceMembers(Set(userAsMember))(project.parent)

        provisionTestProjects(project, parent) >>
          finder
            .findProject(project.slug, user.some)
            .asserting(_ shouldBe project.to(kgProjectConverter).some)
      }

    "return details of the project with the given slug without info about the parent " +
      "if the user has no rights to access the parent project" in projectsDSConfig.use { implicit pcc =>
        val user = authUsers.generateOne
        val project = Gen
          .oneOf(
            renkuProjectWithParentEntities(visibilityNonPublic)
              .modify(replaceMembers(Set(projectMemberEntities(Gen.const(user.id.some)).generateOne))),
            nonRenkuProjectWithParentEntities(visibilityNonPublic)
              .modify(replaceMembers(Set(projectMemberEntities(Gen.const(user.id.some)).generateOne)))
          )
          .generateOne

        val parent = (replaceVisibility[Project](to = Visibility.Private) andThen removeMembers())(project.parent)

        provisionTestProjects(project, parent) >>
          finder
            .findProject(project.slug, Some(user))
            .asserting(_ shouldBe Some(project.to(kgProjectConverter).copy(maybeParent = None)))
      }

    "return None if there's no project with the given slug" in projectsDSConfig.use { implicit pcc =>
      finder.findProject(projectSlugs.generateOne, authUsers.generateOption).asserting(_ shouldBe None)
    }
  }

  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private def finder(implicit pcc: ProjectsConnectionConfig) = new KGProjectFinderImpl[IO](pcc)
}
