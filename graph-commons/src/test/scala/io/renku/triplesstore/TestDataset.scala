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

package io.renku.triplesstore

import cats.effect.IO
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.EntityId
import io.renku.triplesstore.client.http.{RowDecoder, SparqlClient}
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.util.JenaServerSupport
import org.typelevel.log4cats.Logger

trait TestDataset {
  self: JenaServerSupport =>

  def runSelect(
      query: SparqlQuery
  )(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[List[Map[String, String]]] =
    SparqlClient[IO](dcc.toCC()).use {
      _.queryDecode(query)
    }

  def runUpdate(query: SparqlQuery)(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[Unit] =
    SparqlClient[IO](dcc.toCC()).use {
      _.update(query)
    }

  def runUpdates(queries: Seq[SparqlQuery])(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[Unit] =
    queries.traverse_(runUpdate)

  def insert(quad: Quad)(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[Unit] =
    runUpdate {
      SparqlQuery.ofUnsafe("insert quad", sparql"INSERT DATA { $quad }")
    }

  def insert(quads: Seq[Quad])(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[Unit] =
    quads.toList.traverse_(insert)

  def triplesCount(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[Long] =
    runSelect(
      SparqlQuery.ofUnsafe("triples count", "SELECT (COUNT(?s) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }")
    ).map(_.headOption.map(_.apply("count")).flatMap(_.toLongOption).getOrElse(0L))

  def triplesCount(graphId: EntityId)(implicit dcc: DatasetConnectionConfig, L: Logger[IO]): IO[Long] =
    runSelect(
      SparqlQuery.ofUnsafe("triples count on graph",
                           s"SELECT (COUNT(?s) AS ?count) WHERE { GRAPH ${graphId.sparql} { ?s ?p ?o } }"
      )
    ).map(_.headOption.map(_.apply("count")).flatMap(_.toLongOption).getOrElse(0L))

  private implicit lazy val defaultDecoder: RowDecoder[Map[String, String]] =
    RowDecoder.fromDecoder { cur =>
      cur.keys
        .map(_.toList.map(k => cur.downField(k).downField("value").as[String].tupleLeft(k)).sequence.map(_.toMap))
        .getOrElse(Map.empty[String, String].asRight[DecodingFailure])
    }
}
