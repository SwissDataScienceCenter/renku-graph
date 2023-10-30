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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria
import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.GitLabClient
import io.renku.http.rest.paging.model.{Page, Total}
import io.renku.knowledgegraph.users.projects.model.Project
import org.typelevel.log4cats.Logger

private trait GLProjectFinder[F[_]] {
  def findProjectsInGL(criteria: Criteria): F[List[model.Project.NotActivated]]
}

private object GLProjectFinder {
  def apply[F[_]: Async: Parallel: GitLabClient: Logger]: F[GLProjectFinder[F]] =
    new GLProjectFinderImpl[F].pure[F].widen

  val requestPageSize: Page = Page(100)
}

private class GLProjectFinderImpl[F[_]: Async: Parallel: GitLabClient: Logger] extends GLProjectFinder[F] {

  import GLProjectFinder.requestPageSize
  import GitLabClient.maybeTotalPages
  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._
  import org.http4s.implicits._

  override def findProjectsInGL(criteria: Criteria): F[List[Project.NotActivated]] =
    findProjects(criteria) >>= {
      case (projects, Some(total)) if total.value == 1 => projects.pure[F]
      case (projects, Some(total))                     => fetchAllOtherPagesPar(criteria, total).map(projects ::: _)
      case (_, None) =>
        new Exception(s"User's Projects without 'X-Total-Pages'").raiseError[F, List[Project.NotActivated]]
    }

  private def fetchAllOtherPagesPar(criteria: Criteria, total: Total) =
    (2 to total.value).toList
      .map(p => findProjects(criteria, Page(p)).map(_._1))
      .parSequence
      .map(_.reduce(_ ::: _))

  private def findProjects(criteria: Criteria,
                           page:     Page = Page.first
  ): F[(List[Project.NotActivated], Option[Total])] =
    GitLabClient[F]
      .get(
        uri"projects"
          .withQueryParam("membership", true)
          .withQueryParam("min_access_level", 40) // Maintainer
          .withQueryParam("page", page)
          .withQueryParam("per_page", requestPageSize),
        "user-projects"
      )(mapResponse)(criteria.maybeUser.map(_.accessToken))

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(List[Project.NotActivated], Option[Total])]] = {
    case (Ok, _, resp)    => (resp.as[List[Project.NotActivated]], maybeTotalPages(resp)).mapN(_ -> _)
    case (NotFound, _, _) => (List.empty[Project.NotActivated], Option.empty[Total]).pure[F]
  }

  private implicit lazy val projectDecoder: EntityDecoder[F, List[Project.NotActivated]] = {

    implicit val decoder: Decoder[Project.NotActivated] = cursor =>
      for {
        id              <- cursor.downField("id").as[projects.GitLabId]
        name            <- cursor.downField("name").as[projects.Name]
        slug            <- cursor.downField("path_with_namespace").as[projects.Slug]
        maybeVisibility <- cursor.downField("visibility").as[Option[projects.Visibility]]
        dateCreated     <- cursor.downField("created_at").as[projects.DateCreated]
        maybeCreatorId  <- cursor.downField("creator_id").as[Option[persons.GitLabId]]
        keywords        <- cursor.downField("topics").as[List[Option[projects.Keyword]]].map(_.flatten)
        maybeDesc       <- cursor.downField("description").as[Option[projects.Description]]
      } yield model.Project.NotActivated(id,
                                         name,
                                         slug,
                                         maybeVisibility.getOrElse(projects.Visibility.Public),
                                         dateCreated,
                                         maybeCreatorId,
                                         maybeCreator = None,
                                         keywords.sorted,
                                         maybeDesc
      )

    jsonOf[F, List[Project.NotActivated]]
  }
}
