package io.renku.projectauth.util

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{RenkuUrl, Schemas, persons}
import io.renku.projectauth.{Generators, ProjectAuth, ProjectAuthService, ProjectAuthServiceSupport}
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
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
            |  $snippet
            |  Graph ${ProjectAuth.graph} {
            |    ${SparqlSnippets.projectId} a schema:Project;
            |         renku:visibility ?visibility;
            |         renku:slug ?slug;
            |         renku:memberRole ?memberRole.
            |  }
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
      val userIds = data.flatMap(x => x.members).map(_.gitLabId.value)
      for {
        nonExistingUser <- IO {
                             Gen
                               .oneOf(1000 to 100000)
                               .suchThat(id => !userIds.contains(id))
                               .map(persons.GitLabId(_))
                               .generateOne
                           }
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
      val userIds = data.flatMap(x => x.members).map(_.gitLabId).toSet
      for {
        existingUser <- IO(Gen.oneOf(userIds).generateOne)
        myPrivateProj =
          data.filter(p => p.visibility == Visibility.Private && p.members.map(_.gitLabId).contains(existingUser))
        r <- client.queryDecode[ProjectAuthDataRow](
               selectFragment(SparqlSnippets.visibleProjects(existingUser.some, Visibility.all))
             )
        _ = r.map(_.visibility).max shouldBe data.map(_.visibility).max

        found = Stream.emits(r).through(ProjectAuthDataRow.collect).toList
        _     = found.filter(_.visibility == Visibility.Private).toSet shouldBe myPrivateProj.toSet
      } yield ()
    }
  }
}
