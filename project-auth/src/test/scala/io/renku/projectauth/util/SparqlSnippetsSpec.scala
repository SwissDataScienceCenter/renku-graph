package io.renku.projectauth.util

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{RenkuUrl, Schemas, persons}
import io.renku.projectauth.{Generators, ProjectAuth, ProjectAuthData, ProjectAuthService, ProjectAuthServiceSupport}
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
            |     ${SparqlSnippets.projectId} a schema:Project;
            |         renku:visibility ?visibility;
            |         renku:slug ?slug;
            |         renku:memberRole ?memberRole.
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
    clientAndData.use { case (client, _) =>
      for {
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.visibleProjects(None, Visibility.all))
             )
        _ = r.map(_.visibility).toSet shouldBe Set(Visibility.Public)
      } yield ()
    }
  }

  it should "select public and internal if a user is given that is no member" in {
    clientAndData.use { case (client, data) =>
      for {
        nonExistingUser <- IO(selectUserNotIn(data).generateOne)
        nonPrivate = data.filter(p => p.visibility != Visibility.Private)
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.visibleProjects(nonExistingUser.some, Visibility.all))
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
               selectFragment(SparqlSnippets.visibleProjects(existingUser.some, Visibility.all))
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
               selectFragment(SparqlSnippets.visibleProjects(None, internal))
             )
        _ = r shouldBe Nil
      } yield ()
    }
  }

  it should "select public projects if no visibilities and no user are given" in {
    clientAndData.use { case (client, data) =>
      for {
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.visibleProjects(None, Set.empty))
             )
        expected = data.filter(projectFilter(None, Visibility.all)).sortBy(_.slug)
        found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _        = found                         shouldBe expected
        _        = found.map(_.visibility).toSet shouldBe Set(Visibility.Public)
      } yield ()
    }
  }

  it should "select all possible projects if no visibilities are given" in {
    clientAndData.use { case (client, data) =>
      for {
        user <- IO(selectUserFrom(data).generateSome)
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.visibleProjects(user, Set.empty))
             )
        expected = data.filter(projectFilter(user, Visibility.all)).sortBy(_.slug)
        found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _        = found shouldBe expected
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
                 selectFragment(SparqlSnippets.visibleProjects(existingUser, givenVisibility))
               )
          expected = data.filter(projectFilter(existingUser, givenVisibility)).sortBy(_.slug)
          found    = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
          _        = found shouldBe expected
        } yield ()
      }
    }
  }

  def projectFilter(user: Option[persons.GitLabId], givenVisibility: Set[Visibility])(p: ProjectAuthData): Boolean =
    givenVisibility.contains(p.visibility) &&
      (p.visibility != Visibility.Internal || user.isDefined) &&
      (p.visibility != Visibility.Private || user.exists(id => p.members.map(_.gitLabId).contains(id)))
}
