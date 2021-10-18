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

package io.renku.knowledgegraph.datasets.rest

import cats.effect.{ConcurrentEffect, Timer}
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets._
import io.renku.knowledgegraph.datasets.model.DatasetPart
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class PartsFinder[Interpretation[_]: ConcurrentEffect: Timer](
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[Interpretation],
    timeRecorder:            SparqlQueryTimeRecorder[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder) {

  import PartsFinder._

  def findParts(identifier: Identifier): Interpretation[List[DatasetPart]] =
    queryExpecting[List[DatasetPart]](using = query(identifier))

  private def query(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - parts",
    Prefixes.of(prov -> "prov", schema -> "schema"),
    s"""|SELECT DISTINCT ?partLocation
        |WHERE {
        |  ?dataset a schema:Dataset ;
        |           schema:identifier '$identifier' ;
        |           schema:hasPart ?partResource .
        |
        |  ?partResource a schema:DigitalDocument;
        |                prov:entity / prov:atLocation ?partLocation
        |  
        |  FILTER NOT EXISTS {
        |    ?partResource prov:invalidatedAtTime ?invalidation           
        |  }  
        |}
        |ORDER BY ASC(?partLocation)
        |""".stripMargin
  )
}

private object PartsFinder {

  import io.circe.Decoder

  private implicit val partsDecoder: Decoder[List[DatasetPart]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    implicit val datasetDecoder: Decoder[DatasetPart] = { cursor =>
      for {
        partLocation <- cursor.downField("partLocation").downField("value").as[PartLocation]
      } yield DatasetPart(partLocation)
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetPart])
  }
}
