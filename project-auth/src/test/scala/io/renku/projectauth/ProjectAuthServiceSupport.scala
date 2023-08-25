package io.renku.projectauth

import cats.effect._
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuUrl
import io.renku.triplesstore.client.util.JenaContainerSupport
import org.scalacheck.Gen
import org.scalatest.Suite
import org.typelevel.log4cats.Logger

trait ProjectAuthServiceSupport extends JenaContainerSupport { self: Suite =>

  val datasetName: String = "projectauth"

  def withProjectAuthService(implicit renkuUrl: RenkuUrl, L: Logger[IO]): Resource[IO, ProjectAuthService[IO]] =
    withDataset(datasetName).map(ProjectAuthService[IO](_, renkuUrl))

  def withProjectAuthServiceData(data: Stream[Gen, ProjectAuthData])(implicit
      renkuUrl: RenkuUrl,
      L:        Logger[IO]
  ) =
    withProjectAuthService.flatMap { s =>
      val genData = data.toIO.compile.toList
      val insert  = genData.flatMap(d => Stream.emits(d).through(s.updateAll).compile.drain.as(d))
      Resource.eval(insert).map(data => (s, data))
    }
}
