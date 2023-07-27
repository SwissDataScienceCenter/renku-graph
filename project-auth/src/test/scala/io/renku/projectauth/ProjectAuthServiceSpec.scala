package io.renku.projectauth

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuUrl
import io.renku.projectauth.sparql.ConnectionConfig
import org.http4s.implicits._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class ProjectAuthServiceSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers {
  implicit val logger:   Logger[IO] = Slf4jLogger.getLogger[IO]
  implicit val renkuUrl: RenkuUrl   = RenkuUrl("http://localhost/renku")
  val cc = ConnectionConfig(uri"http://localhost:3030/projects", None, None)

  it should "add data" in {
    ProjectAuthService[IO](cc).use { s =>
      for {
        _ <- Generators.projectAuthDataGen.asStream
               .take(20)
               .toIO
               .through(s.updateAll)
               .compile
               .drain
      } yield ()
    }
  }
}
