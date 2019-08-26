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

package ch.datascience.knowledgegraph.graphql.datasets

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.dataSets._
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.knowledgegraph.graphql.datasets.model._
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class PartsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger) {

  import PartsFinder._

  def findParts(projectPath: ProjectPath, dataSetIdentifier: Identifier): IO[List[DataSetPart]] =
    queryExpecting[List[DataSetPart]](using = query(projectPath, dataSetIdentifier))

  private def query(projectPath: ProjectPath, dataSetIdentifier: Identifier): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT ?partName ?partLocation ?dateCreated
       |WHERE {
       |  ?dataSet dcterms:isPartOf|schema:isPartOf ?project .
       |  FILTER (?project = <${renkuBaseUrl / projectPath}>)
       |  ?dataSet rdf:type <http://schema.org/Dataset> ;
       |           rdfs:label "$dataSetIdentifier" ;
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

  private implicit val partsDecoder: Decoder[List[DataSetPart]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val dataSetDecoder: Decoder[DataSetPart] = { cursor =>
      for {
        partName     <- cursor.downField("partName").downField("value").as[PartName]
        partLocation <- cursor.downField("partLocation").downField("value").as[PartLocation]
        dateCreated  <- cursor.downField("dateCreated").downField("value").as[PartDateCreated]
      } yield DataSetPart(partName, partLocation, dateCreated)
    }

    _.downField("results").downField("bindings").as(decodeList[DataSetPart])
  }
}
