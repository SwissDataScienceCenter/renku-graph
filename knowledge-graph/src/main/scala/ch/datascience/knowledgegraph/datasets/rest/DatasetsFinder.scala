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
import ch.datascience.graph.model.datasets.{Identifier, Name}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Phrase
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetsFinder[Interpretation[_]] {
  def findDatasets(phrase: Phrase): Interpretation[List[(Identifier, Name)]]
}

private class IODatasetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger)
    with DatasetsFinder[IO] {

  import IODatasetsFinder._

  override def findDatasets(phrase: Phrase): IO[List[(Identifier, Name)]] =
    queryExpecting[List[(Identifier, Name)]](using = query(phrase))

  private def query(phrase: Phrase): String =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX text: <http://jena.apache.org/text#>
       |
       |SELECT DISTINCT ?identifier ?name ?dataset
       |WHERE {
       |  {
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             text:query (schema:name '$phrase') ;
       |             schema:name ?name ;
       |             rdfs:label ?identifier
       |  } UNION {
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             text:query (schema:description '$phrase') ;
       |             schema:name ?name ;
       |             rdfs:label ?identifier
       |  } UNION {
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             schema:creator ?creatorResource ;
       |             rdfs:label ?identifier ;
       |             schema:name ?name .
       |    ?creatorResource rdf:type <http://schema.org/Person> ;
       |             text:query (schema:name '$phrase')
       |  }
       |}""".stripMargin
}

private object IODatasetsFinder {
  import io.circe.Decoder

  private implicit val recordsDecoder: Decoder[List[(Identifier, Name)]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val recordDecoder: Decoder[(Identifier, Name)] = { cursor =>
      for {
        id   <- cursor.downField("identifier").downField("value").as[Identifier]
        name <- cursor.downField("name").downField("value").as[Name]
      } yield id -> name
    }

    _.downField("results").downField("bindings").as(decodeList[(Identifier, Name)])
  }
}
