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
import cats.implicits._
import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.dataSets.Identifier
import ch.datascience.graph.model.projects.{FullProjectPath, ProjectPath}
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure
import model._

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try

private class ProjectsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger) {

  import ProjectsFinder._

  def findProjects(projectPath: ProjectPath, dataSetIdentifier: Identifier): IO[List[DataSetProject]] =
    queryExpecting[List[DataSetProject]](using = query(projectPath, dataSetIdentifier))

  private def query(projectPath: ProjectPath, dataSetIdentifier: Identifier): String =
    s"""
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT DISTINCT ?isPartOf
       |WHERE {
       |  {
       |    SELECT ?dataSet
       |    WHERE {
       |      ?dataSet dcterms:isPartOf|schema:isPartOf ?project .
       |      FILTER (?project = <${renkuBaseUrl / projectPath}>)
       |      ?dataSet rdf:type <http://schema.org/Dataset> ;
       |               rdfs:label "$dataSetIdentifier" .
       |    }
       |  }
       |  {
       |    ?dataSet dcterms:isPartOf|schema:isPartOf ?isPartOf .
       |  } UNION {
       |    ?dataSet schema:url ?dataSetUrl .
       |    ?otherDataSet rdf:type <http://schema.org/Dataset> ;
       |                  schema:url ?dataSetUrl ;
       |                  dcterms:isPartOf|schema:isPartOf ?isPartOf .
       |  }
       |}
       |ORDER BY ASC(?isPartOf)
       |""".stripMargin
}

private object ProjectsFinder {

  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DataSetProject]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def toProjectName(projectPath: FullProjectPath) =
      projectPath
        .to[Try, ProjectPath]
        .toEither
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

    implicit val projectDecoder: Decoder[DataSetProject] = { cursor =>
      for {
        name <- cursor.downField("isPartOf").downField("value").as[FullProjectPath].flatMap(toProjectName)
      } yield DataSetProject(name)
    }

    _.downField("results").downField("bindings").as(decodeList[DataSetProject])
  }
}
