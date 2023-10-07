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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.{GraphClass, Schemas}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.UpdateQueryMigration
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private object DatasetSearchTitleMigration {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    UpdateQueryMigration[F](name, query).widen

  private lazy val name = Migration.Name("Insert dataset name into the dataset search graph")

  private[migrations] lazy val query = SparqlQuery.of(
    name.asRefined,
    Prefixes.of(Schemas.schema -> "schema", Schemas.renku -> "renku"),
    sparql"""|INSERT {
             |  GRAPH ${GraphClass.Datasets.id} {
             |    ?sameAs schema:name ?dsName
             |  }
             |}
             |WHERE {
             |  SELECT ?sameAs (SAMPLE(?name) AS ?dsName)
             |  WHERE {
             |    GRAPH ${GraphClass.Datasets.id} {
             |      ?sameAs a renku:DiscoverableDataset;
             |              renku:datasetProjectLink ?link.
             |
             |      ?link renku:project ?projectId;
             |            renku:dataset ?dsId
             |    }
             |
             |    GRAPH ?projectId {
             |      ?dsId schema:name ?name
             |    }
             |  }
             |  GROUP BY ?sameAs
             |}
             |""".stripMargin
  )
}