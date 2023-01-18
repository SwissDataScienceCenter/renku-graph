/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.datasets
package details

import Dataset.DatasetPart
import cats.MonadThrow
import cats.effect.Async
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.renku.graph.model.GraphClass
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait PartsFinder[F[_]] {
  def findParts(dataset: Dataset): F[List[DatasetPart]]
}

private class PartsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with PartsFinder[F] {

  import PartsFinderImpl._

  def findParts(dataset: Dataset): F[List[DatasetPart]] =
    queryExpecting[List[DatasetPart]](query(dataset))

  private def query(ds: Dataset) = SparqlQuery.of(
    name = "ds by id - parts",
    Prefixes of (prov -> "prov", schema -> "schema"),
    s"""|SELECT DISTINCT ?partLocation
        |WHERE {
        |  GRAPH <${GraphClass.Project.id(ds.project.id)}> {
        |    ?dataset a schema:Dataset ;
        |             schema:identifier '${ds.id}' ;
        |             schema:hasPart ?partResource.
        |
        |    ?partResource a schema:DigitalDocument;
        |                  prov:entity/prov:atLocation ?partLocation.
        |    
        |    FILTER NOT EXISTS {
        |      ?partResource prov:invalidatedAtTime ?invalidation           
        |    }
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

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig): F[PartsFinder[F]] =
    MonadThrow[F].catchNonFatal(new PartsFinderImpl[F](storeConfig))
}
