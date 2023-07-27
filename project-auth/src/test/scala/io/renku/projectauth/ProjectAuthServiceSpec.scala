package io.renku.projectauth

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.graph.model.RenkuUrl
import io.renku.projectauth.sparql.ConnectionConfig
import io.renku.triplesstore.ExternalJenaForSpec
import org.http4s.implicits._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectAuthServiceSpec extends AsyncFlatSpec with AsyncIOSpec with ExternalJenaForSpec with should.Matchers {

  implicit val renkuUrl: RenkuUrl = RenkuUrl("http://localhost/renku")
  val cc = ConnectionConfig(uri"http://localhost:3030", None)

  it should "add data" in {
    ProjectAuthService[IO](cc).use { s =>
      for {
        projects <- IO(
                      Generators.projectAuthDataStream
                        .take(20)
                        .translate(Generators.genToIO)
                    )
        _ <- projects.through(s.updateAll).compile.drain
      } yield ()
    }
  }
}
