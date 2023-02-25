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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.Async
import cats.syntax.all._
import cats.FlatMap
import eu.timepit.refined.auto._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{projects, RenkuUrl}
import io.renku.graph.model.Schemas.renku
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

private trait MigratedProjectsChecker[F[_]] {
  def filterNotMigrated(paths: List[projects.Path]): F[List[projects.Path]]
}

private object MigratedProjectsChecker {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[MigratedProjectsChecker[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    tsClient                <- MigrationsConnectionConfig[F]().map(TSClient[F](_))
  } yield new MigratedProjectsCheckerImpl[F](tsClient)
}

private class MigratedProjectsCheckerImpl[F[_]: FlatMap](tsClient: TSClient[F])(implicit renkuUrl: RenkuUrl)
    extends MigratedProjectsChecker[F] {

  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.client.syntax._
  import tsClient._
  import ResultsDecoder._
  import io.circe.Decoder
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def filterNotMigrated(paths: List[projects.Path]): F[List[projects.Path]] =
    checkWhichMigrated(paths).map(paths diff _)

  private def checkWhichMigrated(paths: List[projects.Path]) =
    queryExpecting(
      SparqlQuery.ofUnsafe(
        show"${MigrationToV10.name} - filter migrated",
        Prefixes of (renku -> "renku"),
        s"""|SELECT DISTINCT ?path
            |WHERE {
            |  ${MigrationToV10.name.asEntityId.asSparql.sparql} renku:migrated ?path
            |  FILTER (?path IN (${paths.map(_.asObject.asSparql.sparql).mkString(", ")}))
            |}
            |ORDER BY ?path
            |""".stripMargin
      )
    )

  private implicit lazy val pathDecoder: Decoder[List[projects.Path]] = ResultsDecoder[List, projects.Path] {
    implicit cur => extract[projects.Path]("path")
  }
}
