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

package ch.datascience.knowledgegraph.datasets.graphql

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{FullProjectPath, ProjectPath}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.datasets.model.Dataset
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class BaseDetailsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger) {

  import BaseDetailsFinder._

  def findBaseDetails(projectPath: ProjectPath): IO[List[Dataset]] =
    queryExpecting[List[Dataset]](using = query(projectPath))

  private def query(projectPath: ProjectPath): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT DISTINCT ?identifier ?name ?description ?dateCreated ?agentEmail ?agentName ?publishedDate
       |WHERE {
       |  ?dataset dcterms:isPartOf|schema:isPartOf ${FullProjectPath(renkuBaseUrl, projectPath).showAs[RdfResource]} .
       |  ?dataset rdf:type <http://schema.org/Dataset> ;
       |           rdfs:label ?identifier ;
       |           schema:name ?name ;
       |           schema:dateCreated ?dateCreated ;
       |           (prov:qualifiedGeneration/prov:activity/prov:agent) ?agentResource .
       |  ?agentResource rdf:type <http://schema.org/Person> ;
       |           schema:email ?agentEmail ;
       |           schema:name ?agentName .
       |  OPTIONAL { ?dataset schema:description ?description } .         
       |  OPTIONAL { ?dataset schema:datePublished ?publishedDate } .         
       |}""".stripMargin
}

private object BaseDetailsFinder {

  import io.circe.Decoder

  private implicit val baseDetailsDecoder: Decoder[List[Dataset]] = {
    import ch.datascience.graph.model.datasets._
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.knowledgegraph.datasets.model._
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val datasetDecoder: Decoder[Dataset] = { cursor =>
      for {
        id                 <- cursor.downField("identifier").downField("value").as[Identifier]
        name               <- cursor.downField("name").downField("value").as[Name]
        dateCreated        <- cursor.downField("dateCreated").downField("value").as[DateCreated]
        agentEmail         <- cursor.downField("agentEmail").downField("value").as[Email]
        agentName          <- cursor.downField("agentName").downField("value").as[UserName]
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
          DatasetCreation(dateCreated, DatasetAgent(agentEmail, agentName)),
          DatasetPublishing(maybePublishedDate, Set.empty),
          part    = List.empty,
          project = List.empty
        )
    }

    _.downField("results").downField("bindings").as(decodeList[Dataset])
  }
}
