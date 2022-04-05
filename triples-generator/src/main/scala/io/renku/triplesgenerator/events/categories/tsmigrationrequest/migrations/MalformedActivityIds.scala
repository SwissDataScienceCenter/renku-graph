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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling.{CleanUpEventsProducer, QueryBasedMigration}
import org.typelevel.log4cats.Logger

private object MalformedActivityIds {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    QueryBasedMigration[F](name, query, CleanUpEventsProducer).widen

  private lazy val name = Migration.Name("Malformed Activity Ids")
  private[migrations] lazy val query = SparqlQuery.of(
    Refined.unsafeApply(name.show),
    Prefixes.of(schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT DISTINCT ?path
        |WHERE {
        |  ?id a prov:Activity;
        |      ^renku:hasActivity/renku:projectPath ?path
        |  BIND (STR(?id) AS ?idString)
        |  FILTER REGEX(?idString, '^((?!/activities/).)*$$')
        |}
        |""".stripMargin
  )
}
