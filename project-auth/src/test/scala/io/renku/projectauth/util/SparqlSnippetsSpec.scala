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

package io.renku.projectauth.util

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{RenkuUrl, Schemas, persons}
import io.renku.projectauth.{Generators, ProjectAuth, ProjectAuthData, ProjectAuthService, ProjectAuthServiceSupport, QueryFilter}
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class SparqlSnippetsSpec extends AsyncFlatSpec with AsyncIOSpec with ProjectAuthServiceSupport with should.Matchers {

  implicit val logger:   Logger[IO] = Slf4jLogger.getLogger[IO]
  implicit val renkuUrl: RenkuUrl   = RenkuUrl("http://localhost/renku")

  override val datasetName = "sparqlsnippetspec"

  def randomData(num: Int) = Generators.projectAuthDataGen.asStream.take(num)

  def selectFragment(snippet: Fragment) =
    sparql"""${"renku" -> Schemas.renku}
            |${"schema" -> Schemas.schema}
            |
            |SELECT ?slug ?visibility ?memberRole
            |WHERE {
            |  Graph ${ProjectAuth.graph} {
            |     ${SparqlSnippets.default.projectId} a schema:Project;
            |         renku:visibility ?visibility;
            |         renku:slug ?slug.
            |
            |         Optional {
            |           ${SparqlSnippets.default.projectId} renku:memberRole ?memberRole.
            |         }
            |  }
            |
            |  $snippet
            |}
            |ORDER BY ?slug
            |""".stripMargin

  def clientAndData = withDataset(datasetName).evalMap { sc =>
    val pa = ProjectAuthService(sc, renkuUrl)
    insertData(pa, randomData(10)).map(data => (sc, data))
  }

  it should "select public projects if no user is given" in {
    clientAndData.use { case (client, data) =>
      for {
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(None, Visibility.all))
             )
        _ = r.map(_.visibility).toSet shouldBe data.filter(_.visibility == Visibility.Public).map(_.visibility).toSet
      } yield ()
    }
  }

  it should "select public and internal if a user is given that is no member" in {
    clientAndData.use { case (client, data) =>
      for {
        nonExistingUser <- IO(selectUserNotIn(data).generateOne)
        nonPrivate = data.filter(p => p.visibility != Visibility.Private)
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(nonExistingUser.some, Visibility.all))
             )
        _ = r.map(_.visibility).max shouldBe nonPrivate.map(_.visibility).max
      } yield ()
    }
  }

  it should "select projects where user is member and public/internal" in {
    clientAndData.use { case (client, data) =>
      for {
        existingUser <- IO(selectUserFrom(data).generateOne)
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(existingUser.some, Visibility.all))
             )
        _ = r.map(_.visibility).max shouldBe data.map(_.visibility).max

        expected = data.filter(projectFilter(existingUser.some, Visibility.all)).sortBy(_.slug)
        found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _        = found shouldBe expected
      } yield ()
    }
  }

  it should "select no projects if given visibilities results in empty constraints" in {
    val internal: Set[Visibility] = Set(Visibility.Internal)
    clientAndData.use { case (client, _) =>
      for {
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(None, internal))
             )
        _ = r shouldBe Nil
      } yield ()
    }
  }

  it should "select public projects if no visibilities and no user are given" in {
    clientAndData.use { case (client, data) =>
      for {
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(None, Set.empty))
             )
        expected = data.filter(projectFilter(None, Visibility.all)).sortBy(_.slug)
        found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _        = found shouldBe expected
      } yield ()
    }
  }

  it should "select all possible projects if no visibilities are given" in {
    clientAndData.use { case (client, data) =>
      for {
        user <- IO(selectUserFrom(data).generateSome)
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(user, Set.empty))
             )
        expected = data.filter(projectFilter(user, Visibility.all)).sortBy(_.slug)
        found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _        = found shouldBe expected
      } yield ()
    }
  }

  it should "select possible projects when no members exist" in {
    def clientAndData = withDataset(datasetName).evalMap { sc =>
      val pa = ProjectAuthService(sc, renkuUrl)
      insertData(pa, randomData(10).map(_.copy(members = Set.empty))).map(data => (sc, data))
    }
    clientAndData.use { case (client, data) =>
      for {
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.default.visibleProjects(None, Visibility.all))
             )
        expected = data.filter(projectFilter(None, Visibility.all)).sortBy(_.slug)
        found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _        = found shouldBe expected
      } yield ()
    }
  }

  it should "update visibility on a project" in {
    clientAndData.use { case (client, data) =>
      val el       = data.head
      val otherVis = (Visibility.all - el.visibility).head
      for {
        _ <- client.update(
               sparql"""${"renku" -> Schemas.renku}
                       |${"schema" -> Schemas.schema}
                       |${SparqlSnippets.default.changeVisibility(el.slug, otherVis)}
                       |""".stripMargin
             )
        r <- ProjectAuthService(client, renkuUrl).getAll(QueryFilter.all.withSlug(el.slug)).compile.lastOrError
        _ = r.visibility shouldBe otherVis
      } yield ()
    }
  }

  val allVisibilities =
    (Visibility.all.map(Set(_)) ++ Visibility.all.toList.permutations.map(_.tail.toSet)) ++ Set(Visibility.all)

  allVisibilities.foreach { givenVisibility =>
    it should s"select projects with given visibility $givenVisibility" in {
      clientAndData.use { case (client, data) =>
        for {
          existingUser <- IO(selectUserFrom(data).generateSome)
          r <- client.queryDecode[ProjectAuthDataRow](
                 selectFragment(SparqlSnippets.default.visibleProjects(existingUser, givenVisibility))
               )
          expected = data.filter(projectFilter(existingUser, givenVisibility)).sortBy(_.slug)
          found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
          _        = found shouldBe expected
        } yield ()
      }
    }
  }

  def projectFilter(user: Option[persons.GitLabId], givenVisibility: Set[Visibility])(p: ProjectAuthData): Boolean =
    givenVisibility.contains(p.visibility) && (p.visibility match {
      case Visibility.Public   => true
      case Visibility.Internal => user.isDefined
      case Visibility.Private  => user.exists(id => p.members.map(_.gitLabId).contains(id))
    })

}
