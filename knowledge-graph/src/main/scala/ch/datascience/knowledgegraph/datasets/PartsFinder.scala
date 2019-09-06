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

package ch.datascience.knowledgegraph.datasets

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets._
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import model._

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class PartsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger) {

  import PartsFinder._

  def findParts(identifier: Identifier): IO[List[DatasetPart]] =
    queryExpecting[List[DatasetPart]](using = query(identifier))

  private def query(identifier: Identifier): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |
       |SELECT ?partName ?partLocation ?dateCreated
       |WHERE {
       |  ?dataset rdf:type <http://schema.org/Dataset> ;
       |           rdfs:label "$identifier" ;
       |           schema:hasPart ?partResource .
       |  ?partResource rdf:type <http://schema.org/DigitalDocument> ;
       |                schema:name ?partName ;         
       |                prov:atLocation ?partLocation ;         
       |                schema:dateCreated ?dateCreated .         
       |}
       |ORDER BY ASC(?partName)
       |""".stripMargin
}

private object PartsFinder {

  import io.circe.Decoder

  private implicit val partsDecoder: Decoder[List[DatasetPart]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val datasetDecoder: Decoder[DatasetPart] = { cursor =>
      for {
        partName     <- cursor.downField("partName").downField("value").as[PartName]
        partLocation <- cursor.downField("partLocation").downField("value").as[PartLocation]
        dateCreated  <- cursor.downField("dateCreated").downField("value").as[PartDateCreated]
      } yield DatasetPart(partName, partLocation, dateCreated)
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetPart])
  }
}
