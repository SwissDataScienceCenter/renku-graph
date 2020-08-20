/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets.rest

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{Identifier, Keyword}
import ch.datascience.knowledgegraph.datasets.model.Dataset
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.HCursor

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class BaseDetailsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) {

  import BaseDetailsFinder._

  def findBaseDetails(identifier: Identifier): IO[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = query(identifier)) flatMap toSingleDataset

  private def query(identifier: Identifier) = SparqlQuery(
    name = "ds by id - base details",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?datasetId ?identifier ?name ?alternateName ?url (?topmostSameAs AS ?sameAs) ?description ?publishedDate 
        |WHERE {
        |  {
        |    SELECT ?topmostSameAs
        |    WHERE {
        |      {
        |        ?l0 rdf:type <http://schema.org/Dataset>;
        |            schema:identifier "$identifier"
        |      } {
        |        {
        |          {
        |            ?l0 schema:sameAs+/schema:url ?l1.
        |            FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
        |            BIND (?l1 AS ?topmostSameAs)
        |          } UNION {
        |            ?l0 rdf:type <http://schema.org/Dataset>.
        |            FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
        |            BIND (?l0 AS ?topmostSameAs)
        |          }
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1.
        |          ?l1 schema:sameAs+/schema:url ?l2
        |          FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
        |          BIND (?l2 AS ?topmostSameAs)
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1.
        |          ?l1 schema:sameAs+/schema:url ?l2.
        |          ?l2 schema:sameAs+/schema:url ?l3
        |          FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
        |          BIND (?l3 AS ?topmostSameAs)
        |        }
        |      }
        |    }
        |    GROUP BY ?topmostSameAs
        |    HAVING (COUNT(*) > 0)
        |  } {
        |    ?datasetId schema:identifier "$identifier" ;
        |               schema:identifier ?identifier ;
        |               rdf:type <http://schema.org/Dataset> ;
        |               schema:name ?name ;
        |               schema:alternateName ?alternateName .
        |    OPTIONAL { ?datasetId schema:url ?url } .
        |    OPTIONAL { ?datasetId schema:description ?description } .
        |    OPTIONAL { ?datasetId schema:datePublished ?publishedDate } .
        |  }
        |}
        |""".stripMargin
  )

  def findKeywords(identifier: Identifier): IO[List[Keyword]] =
    queryExpecting[List[Keyword]](using = queryKeywords(identifier)).flatMap(s => ME.pure(s))

  private def queryKeywords(identifier: Identifier) = SparqlQuery(
    name = "ds by id - keyword details",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?datasetId ?keyword
        |WHERE {
        |  {
        |    SELECT ?topmostSameAs
        |    WHERE {
        |      {
        |        ?l0 rdf:type <http://schema.org/Dataset>;
        |            schema:identifier "$identifier"
        |      } {
        |        {
        |          {
        |            ?l0 schema:sameAs+/schema:url ?l1.
        |            FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
        |            BIND (?l1 AS ?topmostSameAs)
        |          } UNION {
        |            ?l0 rdf:type <http://schema.org/Dataset>.
        |            FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
        |            BIND (?l0 AS ?topmostSameAs)
        |          }
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1.
        |          ?l1 schema:sameAs+/schema:url ?l2
        |          FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
        |          BIND (?l2 AS ?topmostSameAs)
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1.
        |          ?l1 schema:sameAs+/schema:url ?l2.
        |          ?l2 schema:sameAs+/schema:url ?l3
        |          FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
        |          BIND (?l3 AS ?topmostSameAs)
        |        }
        |      }
        |    }
        |    GROUP BY ?topmostSameAs
        |    HAVING (COUNT(*) > 0)
        |  } {
        |    ?datasetId schema:identifier "$identifier" ;
        |               schema:keywords ?keyword .
        |  }
        |}ORDER BY ASC(?keyword)
        |""".stripMargin
  )

  private lazy val toSingleDataset: List[Dataset] => IO[Option[Dataset]] = {
    case Nil            => ME.pure(None)
    case dataset +: Nil => ME.pure(Some(dataset))
    case datasets       => ME.raiseError(new RuntimeException(s"More than one dataset with ${datasets.head.id} id"))
  }
}

private object BaseDetailsFinder {
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.Decoder

  private[rest] implicit val maybeRecordDecoder: Decoder[List[Dataset]] = {
    import Decoder._
    import ch.datascience.graph.model.datasets._
    import ch.datascience.knowledgegraph.datasets.model._

    def extract[T](property: String, from: HCursor)(implicit decoder: Decoder[T]): Result[T] =
      from.downField(property).downField("value").as[T]

    val dataset: Decoder[Dataset] = { cursor =>
      for {
        id                 <- extract[String]("datasetId", from = cursor)
        identifier         <- extract[Identifier]("identifier", from = cursor)
        title              <- extract[Title]("name", from = cursor)
        name               <- extract[Name]("alternateName", from = cursor)
        maybeUrl           <- extract[Option[String]]("url", from = cursor).map(blankToNone).flatMap(toOption[Url])
        maybeSameAs        <- extract[Option[String]]("sameAs", from = cursor).map(blankToNone).flatMap(toOption[SameAs])
        maybePublishedDate <- extract[Option[PublishedDate]]("publishedDate", from = cursor)
        maybeDescription <- extract[Option[String]]("description", from = cursor)
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield Dataset(
        identifier,
        title,
        name,
        maybeSameAs getOrElse SameAs(id),
        maybeUrl,
        maybeDescription,
        DatasetPublishing(maybePublishedDate, Set.empty),
        parts    = List.empty,
        projects = List.empty,
        keywords = List.empty
      )
    }

    _.downField("results").downField("bindings").as(decodeList(dataset))
  }

  private implicit val keywordsDecoder: Decoder[List[Keyword]] = {

    implicit val keywordDecoder: Decoder[Keyword] = { cursor =>
      for {
        keywordString <- cursor.downField("keyword").downField("value").as[String]
      } yield Keyword(keywordString)
    }

    _.downField("results").downField("bindings").as(decodeList[Keyword])
  }
}
