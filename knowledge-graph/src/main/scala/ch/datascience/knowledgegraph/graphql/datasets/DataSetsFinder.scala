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
import ch.datascience.graph.model.dataSets.{DataSetCreatedDate, DataSetId, DataSetName}
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.graph.model.users.{Email, Name}
import ch.datascience.knowledgegraph.graphql.datasets.model._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait DataSetsFinder[Interpretation[_]] {
  def findDataSets(projectPath: ProjectPath): Interpretation[Set[DataSet]]
}

class IODataSetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger)
    with DataSetsFinder[IO] {

  import IODataSetsFinder._

  override def findDataSets(projectPath: ProjectPath): IO[Set[DataSet]] =
    queryExpecting[Set[DataSet]](using = query(projectPath))

  private def query(projectPath: ProjectPath): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT ?dataSetId ?dataSetName ?dataSetCreationDate ?dataSetCreatorEmail ?dataSetCreatorName
       |WHERE {
       |  ?dataSet dcterms:isPartOf|schema:isPartOf ?project .
       |  FILTER (?project = <${renkuBaseUrl / projectPath}>)
       |  ?dataSet rdf:type <http://schema.org/Dataset> ;
       |           rdfs:label ?dataSetId ;
       |           schema:name ?dataSetName ;
       |           schema:dateCreated ?dataSetCreationDate ;
       |           schema:creator ?creatorResource .
       |  ?creatorResource rdf:type <http://schema.org/Person> ;
       |           schema:email ?dataSetCreatorEmail ;
       |           schema:name ?dataSetCreatorName .
       |}""".stripMargin
}

object IODataSetsFinder {

  def apply(
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[DataSetsFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IODataSetsFinder(config, renkuBaseUrl, logger)

  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.Decoder

  private implicit val dataSetsDecoder: Decoder[Set[DataSet]] = {

    implicit lazy val dataSetDecoder: Decoder[DataSet] = { cursor =>
      for {
        id           <- cursor.downField("dataSetId").downField("value").as[DataSetId]
        name         <- cursor.downField("dataSetName").downField("value").as[DataSetName]
        creationDate <- cursor.downField("dataSetCreationDate").downField("value").as[DataSetCreatedDate]
        creatorEmail <- cursor.downField("dataSetCreatorEmail").downField("value").as[Email]
        creatorName  <- cursor.downField("dataSetCreatorName").downField("value").as[Name]
      } yield DataSet(id, name, DataSetCreation(creationDate, DataSetCreator(creatorEmail, creatorName)))
    }

    _.downField("results").downField("bindings").as[List[DataSet]].map(_.toSet)
  }
}
