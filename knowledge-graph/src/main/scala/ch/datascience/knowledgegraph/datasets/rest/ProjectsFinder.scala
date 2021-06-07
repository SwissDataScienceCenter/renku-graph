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
import cats.syntax.all._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.datasets.Identifier
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.knowledgegraph.datasets.model.DatasetProject
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.Try

private class ProjectsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder) {

  import ProjectsFinder._

  def findUsedIn(identifier: Identifier): IO[List[DatasetProject]] =
    queryExpecting[List[DatasetProject]](using = query(identifier))

  private def query(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - projects",
    Prefixes.of(rdf -> "rdf", schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT DISTINCT ?projectId ?projectName
        |WHERE {
        |  {
        |    SELECT ?allDsId ?projectId (MIN(?dateCreated) AS ?minDateCreated)
        |    WHERE {
        |      ?dsId a schema:Dataset;
        |            schema:identifier '$identifier';
        |            prov:atLocation ?location;
        |            renku:topmostSameAs ?topmostSameAs.
        |            
        |      BIND(CONCAT(?location, "/metadata.yml") AS ?metaDataLocation) .
        |      FILTER NOT EXISTS {
        |      # Removing dataset that have an activity that invalidates them
        |      ?deprecationEntity a prov:Entity;
        |                         prov:atLocation ?metaDataLocation;
        |                         prov:wasInvalidatedBy ?invalidationActivity;
        |                         schema:isPartOf ?projectId.
        |      }  
        |            
        |      ?allDsId a schema:Dataset;
        |               renku:topmostSameAs ?topmostSameAs;
        |               schema:isPartOf ?projectId.
        |      ?projectId schema:dateCreated ?dateCreated.
        |    }
        |    GROUP BY ?allDsId ?projectId
        |  }
        |  
        |  ?allDsId a schema:Dataset;
        |           schema:isPartOf ?projectId.
        |  ?projectId a schema:Project;
        |             schema:dateCreated ?minDateCreated;
        |             schema:name ?projectName.
        |}
        |ORDER BY ASC(?projectName)
        |""".stripMargin
  )
}

private object ProjectsFinder {

  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DatasetProject]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def toProjectPath(projectPath: ResourceId) =
      projectPath
        .as[Try, Path]
        .toEither
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

    implicit val projectDecoder: Decoder[DatasetProject] = { cursor =>
      for {
        path <- cursor.downField("projectId").downField("value").as[ResourceId].flatMap(toProjectPath)
        name <- cursor.downField("projectName").downField("value").as[projects.Name]
      } yield DatasetProject(path, name)
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetProject])
  }
}
