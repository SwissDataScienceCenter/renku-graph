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
import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets.Identifier
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{Path, ResourceId, Visibility}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.datasets.model.DatasetProject
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.util.Try

private trait ProjectsFinder[F[_]] {
  def findUsedIn(identifier: Identifier, authContext: AuthContext[Identifier]): F[List[DatasetProject]]
}

private class ProjectsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig)
    with ProjectsFinder[F] {

  import ProjectsFinderImpl._

  def findUsedIn(identifier: Identifier, authContext: AuthContext[Identifier]): F[List[DatasetProject]] =
    queryExpecting[List[DatasetProject]](using = query(identifier, authContext.maybeAuthUser))

  private def query(identifier: Identifier, maybeAuthUser: Option[AuthUser]) = SparqlQuery.of(
    name = "ds by id - projects",
    Prefixes.of(schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT DISTINCT ?projectId ?projectName
        |WHERE {
        |  ?dsId a schema:Dataset;
        |        schema:identifier '$identifier';
        |        renku:topmostSameAs ?topmostSameAs.
        |  
        |  ?allDsId a schema:Dataset;
        |           renku:topmostSameAs ?topmostSameAs;
        |           ^renku:hasDataset ?projectId.
        |  ${allowedProjectFilterQuery(maybeAuthUser)}
        |  FILTER NOT EXISTS {
        |    ?projectDatasets prov:wasDerivedFrom / schema:url ?allDsId;
        |                     ^renku:hasDataset ?projectId. 
        |  }
        |  FILTER NOT EXISTS {
        |    ?allDsId prov:invalidatedAtTime ?invalidationTime .
        |  }  
        |  ?projectId schema:name ?projectName .
        |}
        |ORDER BY ASC(?projectName)
        |""".stripMargin
  )

  private lazy val allowedProjectFilterQuery: Option[AuthUser] => String = {
    case Some(user) =>
      s"""|?projectId renku:projectVisibility ?parentVisibility .
          |OPTIONAL {
          |  ?projectId schema:member/schema:sameAs ?memberId.
          |  ?memberId schema:additionalType 'GitLab';
          |            schema:identifier ?userGitlabId .
          |}
          |FILTER ( ?parentVisibility = '${Visibility.Public.value}' || ?userGitlabId = ${user.id.value} )
          |""".stripMargin
    case _ =>
      s"""|?projectId renku:projectVisibility ?parentVisibility .
          |FILTER(?parentVisibility = '${Visibility.Public.value}')
          |""".stripMargin
  }
}

private object ProjectsFinderImpl {

  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DatasetProject]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._

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

private object ProjectsFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      rdfStoreConfig: RdfStoreConfig
  ): F[ProjectsFinder[F]] = MonadThrow[F].catchNonFatal(new ProjectsFinderImpl[F](rdfStoreConfig))
}
