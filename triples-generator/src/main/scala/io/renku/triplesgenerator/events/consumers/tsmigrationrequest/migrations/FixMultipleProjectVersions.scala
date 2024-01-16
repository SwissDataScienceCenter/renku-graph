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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.versions.SchemaVersion
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.config.VersionCompatibilityConfig
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.UpdateQueryMigration
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private object FixMultipleProjectVersions {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    VersionCompatibilityConfig
      .fromConfigF[F](ConfigFactory.load())
      .map(_.asVersionPair.schemaVersion)
      .flatMap(version => UpdateQueryMigration[F](name, exclusive = false, query(version)).widen)
      .widen

  private lazy val name = Migration.Name("Remove multiple values for project version")

  private[migrations] def query(currentVersion: SchemaVersion) = SparqlQuery.of(
    name.asRefined,
    Prefixes of schema -> "schema",
    sparql"""|DELETE { GRAPH ?id { ?id schema:schemaVersion ?schemaVersion } }
             |WHERE {
             |  GRAPH ?id {
             |    {
             |      SELECT ?id
             |      WHERE {
             |        ?id a schema:Project;
             |            schema:schemaVersion ?version.
             |      }
             |      GROUP BY ?id
             |      HAVING (COUNT(?version) > 1)
             |    }
             |    ?id schema:schemaVersion ?schemaVersion.
             |    FILTER ( ?schemaVersion != ${currentVersion.asObject} )
             |  }
             |}
             |""".stripMargin
  )
}
