/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.schema
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.Migration
import org.typelevel.log4cats.Logger
import tooling.UpdateQueryMigration

private object MultipleDSDescriptions {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    UpdateQueryMigration[F](name, query).widen

  private lazy val name = Migration.Name("Multiple Descriptions on DS")
  private[migrations] lazy val query = SparqlQuery.of(
    name.asRefined,
    Prefixes of schema -> "schema",
    s"""|DELETE { ?dsId schema:description ?desc }
        |WHERE {
        |  SELECT ?dsId ?desc
        |  WHERE {
        |    {
        |      SELECT ?dsId (SAMPLE(?desc) as ?someDesc)
        |      WHERE {
        |        ?dsId a schema:Dataset;
        |              schema:description ?desc.
        |      }
        |      GROUP BY ?dsId
        |      HAVING (COUNT(?desc) > 1)
        |    }
        |    ?dsId schema:description ?desc.
        |    FILTER ( ?desc != ?someDesc )
        |  }
        |}
        |""".stripMargin
  )
}
