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

package io.renku.knowledgegraph.datasets.rest

import cats.MonadThrow
import cats.effect.Async
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets._
import io.renku.knowledgegraph.datasets.model.DatasetPart
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait PartsFinder[F[_]] {
  def findParts(identifier: Identifier): F[List[DatasetPart]]
}

private class PartsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig)
    with PartsFinder[F] {

  import PartsFinderImpl._

  def findParts(identifier: Identifier): F[List[DatasetPart]] =
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

private object PartsFinderImpl {

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

private object PartsFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](rdfStoreConfig: RdfStoreConfig): F[PartsFinder[F]] =
    MonadThrow[F].catchNonFatal(new PartsFinderImpl[F](rdfStoreConfig))
}
