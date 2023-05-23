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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.projectsgraph

import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait ProgressFinder[F[_]] {
  def findProgressInfo: F[String]
}

private object ProgressFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProgressFinder[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    migrationsDSClient      <- MigrationsConnectionConfig[F]().map(TSClient[F](_))
  } yield new ProgressFinderImpl[F](migrationsDSClient)
}

private class ProgressFinderImpl[F[_]: Sync](migrationsDSClient: TSClient[F])(implicit ru: RenkuUrl)
    extends ProgressFinder[F] {

  import io.renku.triplesstore.ResultsDecoder._

  private val total: Ref[F, Int] = Ref.unsafe[F, Int](0)

  override def findProgressInfo: F[String] = for {
    left  <- findLeftCount
    total <- findTotal
  } yield s"$left left from $total"

  private def findLeftCount =
    migrationsDSClient.queryExpecting[Int](
      SparqlQuery.ofUnsafe(
        show"${ProvisionProjectsGraph.name} - find left",
        Prefixes of (renku -> "renku"),
        s"""|SELECT (COUNT(?path) AS ?count)
            |WHERE {
            |  ${ProvisionProjectsGraph.name.asEntityId.asSparql.sparql} renku:toBeMigrated ?path
            |}
            |""".stripMargin
      )
    )

  private def findTotal: F[Int] =
    total.get >>= {
      case 0       => findLeftCount >>= (v => total.updateAndGet(_ => v))
      case nonZero => nonZero.pure[F]
    }

  private implicit lazy val countDecoder: Decoder[Int] =
    ResultsDecoder.single[Int](implicit cur => extract[String]("count").map(_.toInt))
}
