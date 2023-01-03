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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.config.ServiceVersion
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private[migrations] trait MigrationExecutionRegister[F[_]] {
  def registerExecution(migrationName: Migration.Name): F[Unit]
  def findExecution(migrationName:     Migration.Name): F[Option[ServiceVersion]]
}

private class MigrationExecutionRegisterImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    serviceVersion:  ServiceVersion,
    storeConfig:     MigrationsConnectionConfig
)(implicit renkuUrl: RenkuUrl)
    extends TSClient(storeConfig)
    with MigrationExecutionRegister[F] {

  import MigrationExecutionRegister._
  import eu.timepit.refined.auto._

  override def registerExecution(migrationName: Migration.Name): F[Unit] =
    upload(MigrationExecution(migrationName, serviceVersion).asJsonLD)

  override def findExecution(name: Migration.Name): F[Option[ServiceVersion]] =
    queryExpecting[List[MigrationExecution]] {
      SparqlQuery.of(
        name = "migrations - find execution",
        Prefixes of renku -> "renku",
        s"""|SELECT DISTINCT ?name ?version
            |WHERE {
            |  ?entityId a renku:Migration;
            |            renku:migrationName ?name;
            |            renku:serviceVersion ?version
            |  FILTER (?name = '${name.show}')
            |}
            |""".stripMargin
      )
    }.map {
      case Nil    => Option.empty
      case e :: _ => e.serviceVersion.some
    }
}

private[migrations] object MigrationExecutionRegister {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[MigrationExecutionRegister[F]] = for {
    implicit0(renkuUrl: RenkuUrl) <- RenkuUrlLoader[F]()
    serviceVersion                <- ServiceVersion.readFromConfig[F]()
    storeConfig                   <- MigrationsConnectionConfig[F]()
  } yield new MigrationExecutionRegisterImpl[F](serviceVersion, storeConfig)

  private[migrations] final case class MigrationExecution(migrationName: Migration.Name, serviceVersion: ServiceVersion)

  private[migrations] object MigrationExecution {

    implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[MigrationExecution] =
      JsonLDEncoder.instance { entity =>
        JsonLD.entity(
          EntityId.of((renkuUrl / "migration" / entity.migrationName.asUrlPart).toString),
          EntityTypes of renku / "Migration",
          renku / "migrationName"  -> entity.migrationName.asJsonLD,
          renku / "serviceVersion" -> entity.serviceVersion.asJsonLD
        )
      }

    private implicit class MigrationNameOps(name: Migration.Name) {
      lazy val asUrlPart: String = name.show.replace(' ', '-')
    }
  }

  implicit lazy val jsonDecoder: Decoder[List[MigrationExecution]] = { topCursor =>
    val executions: Decoder[MigrationExecution] = { cursor =>
      for {
        name           <- cursor.downField("name").downField("value").as[Migration.Name]
        serviceVersion <- cursor.downField("version").downField("value").as[ServiceVersion]
      } yield MigrationExecution(name, serviceVersion)
    }
    topCursor.downField("results").downField("bindings").as(decodeList(executions))
  }
}
