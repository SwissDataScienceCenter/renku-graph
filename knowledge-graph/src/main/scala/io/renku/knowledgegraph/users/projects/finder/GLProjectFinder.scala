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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.GitLabClient
import io.renku.knowledgegraph.users.projects.model.Project
import org.typelevel.log4cats.Logger

private trait GLProjectFinder[F[_]] {
  def findProjectsInGL(criteria: Criteria): F[List[model.Project.NotActivated]]
}

private object GLProjectFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GLProjectFinder[F]] =
    GLCreatorFinder[F].map(new GLProjectFinderImpl[F](_))
}

private class GLProjectFinderImpl[F[_]: Async: GitLabClient: Logger](creatorFinder: GLCreatorFinder[F])
    extends GLProjectFinder[F] {

  import cats.syntax.all._
  import creatorFinder._
  import eu.timepit.refined.auto._
  import io.circe._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._
  import org.http4s.implicits._

  override def findProjectsInGL(criteria: Criteria): F[List[Project.NotActivated]] =
    findProjectsAndCreators(criteria) >>= addCreators(criteria)

  private def addCreators(criteria: Criteria): List[ProjectAndCreator] => F[List[Project.NotActivated]] = _.map {
    case (proj, Some(creatorId)) =>
      findCreatorName(creatorId)(criteria.maybeUser.map(_.accessToken)).map(creatorName =>
        proj.copy(maybeCreator = creatorName)
      )
    case (proj, None) => proj.copy(maybeCreator = None).pure[F]
  }.sequence

  private def findProjectsAndCreators(criteria: Criteria): F[List[ProjectAndCreator]] =
    GitLabClient[F].get(uri"users" / criteria.userId.value.show / "projects", "user-projects")(
      mapResponse
    )(criteria.maybeUser.map(_.accessToken))

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[List[ProjectAndCreator]]] = {
    case (Ok, _, response) => response.as[List[ProjectAndCreator]]
    case (NotFound, _, _)  => List.empty[ProjectAndCreator].pure[F]
  }

  private type ProjectAndCreator = (model.Project.NotActivated, Option[persons.GitLabId])

  private implicit lazy val projectDecoder: EntityDecoder[F, List[ProjectAndCreator]] = {

    implicit val decoder: Decoder[ProjectAndCreator] = cursor =>
      for {
        id             <- cursor.downField("id").as[projects.Id]
        name           <- cursor.downField("name").as[projects.Name]
        path           <- cursor.downField("path_with_namespace").as[projects.Path]
        visibility     <- cursor.downField("visibility").as[projects.Visibility]
        dateCreated    <- cursor.downField("created_at").as[projects.DateCreated]
        maybeCreatorId <- cursor.downField("creator_id").as[Option[persons.GitLabId]]
        keywords       <- cursor.downField("topics").as[List[Option[projects.Keyword]]].map(_.flatten)
        maybeDesc      <- cursor.downField("description").as[Option[projects.Description]]
      } yield model.Project.NotActivated(id,
                                         name,
                                         path,
                                         visibility,
                                         dateCreated,
                                         maybeCreator = None,
                                         keywords.sorted,
                                         maybeDesc
      ) -> maybeCreatorId

    jsonOf[F, List[ProjectAndCreator]]
  }
}
