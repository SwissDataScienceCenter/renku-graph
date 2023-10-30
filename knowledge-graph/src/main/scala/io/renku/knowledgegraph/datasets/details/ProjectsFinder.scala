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

package io.renku.knowledgegraph.datasets.details

import cats.MonadThrow
import cats.effect.kernel.Async
import eu.timepit.refined.auto._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects.{ResourceId, Slug, Visibility}
import io.renku.graph.model.{GraphClass, datasets, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.jsonld.syntax._
import io.renku.knowledgegraph.datasets.details.Dataset.DatasetProject
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait ProjectsFinder[F[_]] {
  def findUsedIn(dataset: Dataset, authContext: AuthContext[RequestedDataset]): F[List[DatasetProject]]
}

private class ProjectsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with ProjectsFinder[F] {

  import ProjectsFinderImpl._

  def findUsedIn(dataset: Dataset, authContext: AuthContext[RequestedDataset]): F[List[DatasetProject]] =
    queryExpecting[List[DatasetProject]](query(dataset, authContext.maybeAuthUser))

  private def query(ds: Dataset, maybeAuthUser: Option[AuthUser]) = SparqlQuery.of(
    name = "ds by id - projects",
    Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
    sparql"""|SELECT DISTINCT ?projectId ?projectSlug ?projectName ?projectVisibility ?projectDSId
             |WHERE {
             |  GRAPH ${GraphClass.Project.id(ds.project.id)} {
             |    ${ds.resourceId.asEntityId} a schema:Dataset;
             |                                renku:topmostSameAs ?topmostSameAs
             |  }
             |  
             |  GRAPH ${GraphClass.Datasets.id} {
             |    ?topmostSameAs a renku:DiscoverableDataset;
             |                   renku:datasetProjectLink ?linkId.
             |    ?linkId renku:dataset ?projectDS;
             |            renku:project ?projectId.
             |  }
             |  ${allowedProjectFilterQuery(maybeAuthUser)}
             |  GRAPH ?projectId {
             |    ?projectId schema:name ?projectName;
             |               renku:projectPath ?projectSlug;
             |               renku:projectVisibility ?projectVisibility.
             |    ?projectDS schema:identifier ?projectDSId.
             |  }
             |}
             |ORDER BY ASC(?projectName)
             |""".stripMargin
  )

  private lazy val allowedProjectFilterQuery: Option[AuthUser] => Fragment = { user =>
    SparqlSnippets.default.visibleProjects(user.map(_.id), Visibility.all)
  }
}

private object ProjectsFinderImpl {

  import ResultsDecoder._
  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DatasetProject]] = ResultsDecoder[List, DatasetProject] {
    implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      for {
        id          <- extract[projects.ResourceId]("projectId")
        slug        <- extract[projects.Slug]("projectSlug")
        name        <- extract[projects.Name]("projectName")
        visibility  <- extract[projects.Visibility]("projectVisibility")
        projectDSId <- extract[datasets.Identifier]("projectDSId")
      } yield DatasetProject(id, slug, name, visibility, projectDSId)
  }
}

private object ProjectsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig): F[ProjectsFinder[F]] =
    MonadThrow[F].catchNonFatal(new ProjectsFinderImpl[F](storeConfig))
}
