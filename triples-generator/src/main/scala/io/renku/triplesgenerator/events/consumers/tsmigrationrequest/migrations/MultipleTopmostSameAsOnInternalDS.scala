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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger
import tooling.UpdateQueryMigration

private object MultipleTopmostSameAsOnInternalDS {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    UpdateQueryMigration[F](name, query).widen

  private lazy val name = Migration.Name("Multiple TopmostSameAs on Internal DS")
  private[migrations] lazy val query = SparqlQuery.of(
    name.asRefined,
    Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
    s"""|DELETE { ?dsId renku:topmostSameAs ?top. }
        |WHERE {
        |  SELECT ?dsId ?top
        |  WHERE {
        |    {
        |      SELECT ?dsId
        |      WHERE {
        |        ?dsId a schema:Dataset;
        |                renku:topmostSameAs ?topSameAs.
        |        FILTER EXISTS { ?dsId renku:topmostDerivedFrom ?dsId }
        |        FILTER EXISTS { ?dsId renku:topmostSameAs ?dsId }
        |        FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?de }
        |        FILTER NOT EXISTS { ?dsId schema:sameAs ?sa }
        |      }
        |      GROUP BY ?dsId ?orig
        |      HAVING (COUNT(?topSameAs) > 1)
        |      ORDER BY ?dsId
        |    }
        |    ?dsId a schema:Dataset;
        |          renku:topmostSameAs ?top.
        |    FILTER ( ?dsId != ?top )
        |  }
        |}
        |""".stripMargin
  )
}
