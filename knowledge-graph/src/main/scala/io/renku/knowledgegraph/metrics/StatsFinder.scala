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

package io.renku.knowledgegraph.metrics

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.circe.{Decoder, DecodingFailure}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.tinytypes.{TinyType, TinyTypeFactory}
import org.typelevel.log4cats.Logger

private trait StatsFinder[F[_]] {
  def entitiesCount(): F[Map[EntityLabel, Count]]
}

private class StatsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    renkuConnectionConfig: RenkuConnectionConfig
) extends TSClientImpl[F](renkuConnectionConfig)
    with StatsFinder[F] {

  import EntityCount._
  import io.renku.graph.model.Schemas._

  override def entitiesCount(): F[Map[EntityLabel, Count]] =
    queryExpecting[List[(EntityLabel, Count)]](using = query) map (_.toMap)

  private lazy val query = SparqlQuery.of(
    name = "entities - counts",
    Prefixes.of(prov -> "prov", schema -> "schema", renku -> "renku"),
    s"""|SELECT ?type ?count
        |WHERE {
        |  {
        |    SELECT (schema:Dataset AS ?type) (COUNT(DISTINCT ?id) AS ?count)
        |    WHERE { ?id a schema:Dataset }
        |  } UNION {
        |    SELECT (schema:Project AS ?type) (COUNT(DISTINCT ?id) AS ?count)
        |    WHERE { ?id a schema:Project }
        |  } UNION {
        |    SELECT (prov:Activity AS ?type) (COUNT(DISTINCT ?id) AS ?count)
        |    WHERE { ?id a prov:Activity }
        |  } UNION {
        |    SELECT (prov:Plan AS ?type) (COUNT(DISTINCT ?id) AS ?count)
        |    WHERE { ?id a prov:Plan }
        |  } UNION {
        |    SELECT (schema:Person AS ?type) (COUNT(DISTINCT ?id) AS ?count)
        |    WHERE { 
        |      ?activityId a prov:Activity;
        |                  prov:wasAssociatedWith ?id.
        |      ?id a schema:Person.
        |    }
        |  } UNION {
        |    SELECT (CONCAT(STR(schema:Person), ' with GitLabId') AS ?type) (COUNT(DISTINCT ?id) AS ?count)
        |    WHERE { 
        |      ?activityId a prov:Activity;
        |                  prov:wasAssociatedWith ?id.
        |      ?id a schema:Person;
        |          schema:sameAs/schema:additionalType 'GitLab'.
        |    }
        |  }
        |}
        |""".stripMargin
  )
}

private object EntityCount {

  private[metrics] implicit val countsDecoder: Decoder[List[(EntityLabel, Count)]] = {
    val counts: Decoder[(EntityLabel, Count)] = { cursor =>
      for {
        entityType <- cursor
                        .downField("type")
                        .downField("value")
                        .as[String]
                        .flatMap(convert[String, EntityLabel](EntityLabel))
        count <-
          cursor.downField("count").downField("value").as[Long].flatMap(convert[Long, Count](Count))
      } yield (entityType, count)
    }

    _.downField("results")
      .downField("bindings")
      .as(decodeList(counts))
  }

  private def convert[IN, OUT <: TinyType { type V = IN }](implicit
      tinyTypeFactory: TinyTypeFactory[OUT]
  ): IN => Either[DecodingFailure, OUT] =
    value =>
      tinyTypeFactory
        .from(value)
        .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
}

private object StatsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[StatsFinder[F]] = for {
    config <- RenkuConnectionConfig[F]()
  } yield new StatsFinderImpl[F](config)
}
