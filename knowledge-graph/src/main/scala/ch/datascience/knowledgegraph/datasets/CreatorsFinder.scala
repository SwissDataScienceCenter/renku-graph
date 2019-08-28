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
import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.dataSets._
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import model._

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class CreatorsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger) {

  import CreatorsFinder._

  def findCreators(projectPath: ProjectPath, dataSetIdentifier: Identifier): IO[Set[DataSetCreator]] =
    queryExpecting[List[DataSetCreator]](using = query(projectPath, dataSetIdentifier))
      .map(_.toSet)

  private def query(projectPath: ProjectPath, dataSetIdentifier: Identifier): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT ?creatorEmail ?creatorName 
       |WHERE {
       |  ?dataSet dcterms:isPartOf|schema:isPartOf ?project .
       |  FILTER (?project = <${renkuBaseUrl / projectPath}>)
       |  ?dataSet rdf:type <http://schema.org/Dataset> ;
       |           rdfs:label "$dataSetIdentifier" ;
       |           schema:creator ?creatorResource .
       |  OPTIONAL { ?creatorResource rdf:type <http://schema.org/Person> ;
       |                              schema:email ?creatorEmail . } .         
       |  ?creatorResource rdf:type <http://schema.org/Person> ;
       |                   schema:name ?creatorName .         
       |}""".stripMargin
}

private object CreatorsFinder {

  import io.circe.Decoder

  private implicit val creatorsDecoder: Decoder[List[DataSetCreator]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val dataSetDecoder: Decoder[DataSetCreator] = { cursor =>
      for {
        maybeCreatorEmail <- cursor.downField("creatorEmail").downField("value").as[Option[Email]]
        creatorName       <- cursor.downField("creatorName").downField("value").as[UserName]
      } yield DataSetCreator(maybeCreatorEmail, creatorName)
    }

    _.downField("results").downField("bindings").as(decodeList[DataSetCreator])
  }
}
