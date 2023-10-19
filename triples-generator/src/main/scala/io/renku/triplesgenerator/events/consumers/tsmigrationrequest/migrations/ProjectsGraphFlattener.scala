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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.concatSeparator
import io.renku.graph.model.GraphClass
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import migrations.tooling.RegisteredUpdateQueryMigration
import org.typelevel.log4cats.Logger

private object ProjectsGraphFlattener {

  private lazy val name = Migration.Name("Flatten Projects graph")

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    RegisteredUpdateQueryMigration[F](name, query).widen

  private[migrations] lazy val query = SparqlQuery.of(
    name.asRefined,
    Prefixes of (renku -> "renku", schema -> "schema"),
    sparql"""|DELETE {
             |  GRAPH ${GraphClass.Projects.id} {
             |    ?id renku:keywordsConcat ?currentKeysConcat.
             |    ?id renku:imagesConcat ?currentImgsConcat.
             |  }
             |}
             |INSERT {
             |  GRAPH ${GraphClass.Projects.id} {
             |    ?id renku:keywordsConcat ?keysConcat.
             |    ?id renku:imagesConcat ?imagesConcat.
             |  }
             |}
             |WHERE {
             |   SELECT ?id ?currentKeysConcat ?currentImgsConcat
             |     (GROUP_CONCAT(DISTINCT ?keys; separator=${concatSeparator.asTripleObject}) AS ?keysConcat)
             |     (GROUP_CONCAT(DISTINCT ?img; separator=${concatSeparator.asTripleObject}) AS ?imagesConcat)
             |   WHERE {
             |     GRAPH ${GraphClass.Projects.id} {
             |       OPTIONAL {
             |         ?id schema:keywords ?keys
             |       }
             |       OPTIONAL {
             |         ?id renku:keywordsConcat ?currentKeysConcat.
             |       }
             |       
             |       OPTIONAL {
             |         ?id schema:image ?imageId.
             |         ?imageId schema:position ?imagePosition;
             |                  schema:contentUrl ?imageUrl.
             |         BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?img)
             |       }
             |       OPTIONAL {
             |         ?id renku:imagesConcat ?currentImgsConcat.
             |       }
             |     }
             |   }
             |   GROUP BY ?id ?currentKeysConcat ?currentImgsConcat
             |}
             |""".stripMargin
  )
}
