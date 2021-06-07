/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.datasets._
import ch.datascience.knowledgegraph.datasets.model.DatasetPart
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.graph.Schemas._
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext

private class PartsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder) {

  import PartsFinder._

  def findParts(identifier: Identifier): IO[List[DatasetPart]] =
    queryExpecting[List[DatasetPart]](using = query(identifier))

  private def query(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - parts",
    Prefixes.of(prov -> "prov", rdf -> "rdf", rdfs -> "rdfs", schema -> "schema"),
    s"""|SELECT DISTINCT ?partName ?partLocation
        |WHERE {
        |  ?dataset rdf:type <http://schema.org/Dataset> ;
        |           schema:identifier "$identifier" ;
        |           schema:hasPart ?partResource .
        |  ?partResource a schema:DigitalDocument;
        |                schema:name ?partName ;
        |                prov:atLocation ?partLocation .
        |}
        |ORDER BY ASC(?partName)
        |""".stripMargin
  )
}

private object PartsFinder {

  import io.circe.Decoder

  private implicit val partsDecoder: Decoder[List[DatasetPart]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val datasetDecoder: Decoder[DatasetPart] = { cursor =>
      for {
        partLocation <- cursor.downField("partLocation").downField("value").as[PartLocation]
      } yield DatasetPart(partLocation)
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetPart])
  }
}
