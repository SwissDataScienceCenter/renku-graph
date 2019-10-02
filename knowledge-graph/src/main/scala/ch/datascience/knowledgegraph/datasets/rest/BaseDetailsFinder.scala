/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.Identifier
import ch.datascience.knowledgegraph.datasets.model.Dataset
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.DecodingFailure

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class BaseDetailsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger) {

  import BaseDetailsFinder._

  def findBaseDetails(identifier: Identifier): IO[Option[Dataset]] =
    queryExpecting[Option[Dataset]](using = query(identifier))

  private def query(identifier: Identifier): String =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |
       |SELECT DISTINCT ?identifier ?name ?description ?publishedDate
       |WHERE {
       |  ?dataset rdfs:label "$identifier" ;
       |           rdfs:label ?identifier ;
       |           rdf:type <http://schema.org/Dataset> ;
       |           schema:name ?name .
       |  OPTIONAL { ?dataset schema:description ?description } .         
       |  OPTIONAL { ?dataset schema:datePublished ?publishedDate } .         
       |}""".stripMargin
}

private object BaseDetailsFinder {
  import io.circe.Decoder

  private[rest] implicit val maybeRecordDecoder: Decoder[Option[Dataset]] = {
    import Decoder._
    import ch.datascience.graph.model.datasets._
    import ch.datascience.knowledgegraph.datasets.model._
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val dataset: Decoder[Dataset] = { cursor =>
      for {
        id                 <- cursor.downField("identifier").downField("value").as[Identifier]
        name               <- cursor.downField("name").downField("value").as[Name]
        maybePublishedDate <- cursor.downField("publishedDate").downField("value").as[Option[PublishedDate]]
        maybeDescription <- cursor
                             .downField("description")
                             .downField("value")
                             .as[Option[String]]
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield
        Dataset(
          id,
          name,
          maybeDescription,
          DatasetPublishing(maybePublishedDate, Set.empty),
          part    = List.empty,
          project = List.empty
        )
    }

    _.downField("results").downField("bindings").as(decodeList(dataset)).flatMap {
      case Nil            => Right(None)
      case dataset +: Nil => Right(Some(dataset))
      case manyDatasets   => Left(DecodingFailure(s"More than one dataset with ${manyDatasets.head.id} id", Nil))
    }
  }
}
