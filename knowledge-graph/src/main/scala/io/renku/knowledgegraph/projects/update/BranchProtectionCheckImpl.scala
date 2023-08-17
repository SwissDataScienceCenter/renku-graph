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

package io.renku.knowledgegraph.projects.update

import BranchProtectionCheck._
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status.{NotFound, Ok}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.implicits._
import org.http4s.{Request, Response, Status}

private trait BranchProtectionCheck[F[_]] {
  def canPushToDefaultBranch(slug: projects.Slug, at: AccessToken): F[Boolean]
}

private object BranchProtectionCheck {

  def apply[F[_]: Async: GitLabClient]: BranchProtectionCheck[F] = new BranchProtectionCheckImpl[F]

  case class BranchInfo(default: Boolean, canPush: Boolean)
}

private class BranchProtectionCheckImpl[F[_]: Async: GitLabClient] extends BranchProtectionCheck[F] {

  override def canPushToDefaultBranch(slug: projects.Slug, at: AccessToken): F[Boolean] =
    GitLabClient[F]
      .get(uri"projects" / slug / "repository" / "branches", "project-branches")(mapResponse)(at.some)
      .map(_.exists(branch => branch.default && branch.canPush))

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[List[BranchInfo]]] = {
    case (Ok, _, resp)    => resp.as[List[BranchInfo]]
    case (NotFound, _, _) => List.empty[BranchInfo].pure[F]
  }

  private implicit lazy val itemDecoder: Decoder[BranchInfo] =
    Decoder.forProduct2("default", "can_push")(BranchInfo.apply)
}
