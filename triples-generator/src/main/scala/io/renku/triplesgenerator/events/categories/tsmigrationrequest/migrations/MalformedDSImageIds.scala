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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger
import tooling.{CleanUpEventsProducer, QueryBasedMigration}

private object MalformedDSImageIds {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    QueryBasedMigration[F](name, query, CleanUpEventsProducer).widen

  private lazy val name = Migration.Name("Malformed DS Image Ids")
  private[migrations] lazy val query = SparqlQuery.of(
    name.asRefined,
    Prefixes.of(schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?path
        |WHERE {
        |  ?id a schema:Dataset;
        |      ^renku:hasDataset/renku:projectPath ?path;
        |      schema:image ?imageId.
        |  BIND (STR(?imageId) AS ?imageIdString)
        |  FILTER REGEX(?imageIdString, '(.*/datasets/([a-zA-Z0-9]+[-]+[a-zA-Z0-9]+)+(/.*)*)')
        |}
        |""".stripMargin
  )
}
