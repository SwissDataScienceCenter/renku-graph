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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.lucenereindex

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Triple
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

import java.time.Instant

private trait MigrationStartTimeFinder[F[_]] {
  def findMigrationStartDate: F[Instant]
}

private object MigrationStartTimeFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      migrationName: Migration.Name
  ): F[MigrationStartTimeFinder[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    tsClient                <- MigrationsConnectionConfig.fromConfig[F]().map(TSClient[F](_))
  } yield new MigrationStartTimeFinderImpl[F](migrationName, tsClient)
}

private class MigrationStartTimeFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](migrationName: Migration.Name,
                                                                                         tsClient: TSClient[F]
)(implicit ru: RenkuUrl)
    extends MigrationStartTimeFinder[F] {

  import ResultsDecoder._

  override def findMigrationStartDate: F[Instant] =
    tsClient.queryExpecting[Option[Instant]](startTimeQuery) >>= {
      case Some(time) => time.pure[F]
      case None =>
        val startTime = Instant.now()
        tsClient.updateWithNoResult(startTimeTriple(startTime)).as(startTime)
    }

  private lazy val startTimeQuery =
    SparqlQuery.ofUnsafe(
      show"$migrationName - find start time",
      Prefixes of renku -> "renku",
      sparql"""|SELECT ?time
               |WHERE {
               |  ${migrationName.asEntityId} renku:startTime ?time
               |}
               |""".stripMargin
    )

  private implicit lazy val timeDecoder: Decoder[Option[Instant]] = ResultsDecoder[Option, Instant] { implicit cur =>
    extract[Instant]("time")
  }

  private def startTimeTriple(instant: Instant) = {
    val triple = Triple(migrationName.asEntityId, renku / "startTime", instant.asTripleObject)
    SparqlQuery.ofUnsafe(
      show"$migrationName - store start time",
      s"INSERT DATA {${triple.asSparql.sparql}}"
    )
  }
}
