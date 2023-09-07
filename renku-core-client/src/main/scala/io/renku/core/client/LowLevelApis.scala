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

package io.renku.core.client

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure}
import io.renku.control.Throttler
import io.renku.graph.model.projects
import io.renku.graph.model.versions.SchemaVersion
import io.renku.http.client.{AccessToken, RestClient, UserAccessToken}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Header
import org.http4s.circe._
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait LowLevelApis[F[_]] {
  def getApiVersion(uri: RenkuCoreUri.ForSchema): F[Result[SchemaApiVersions]]
  def getMigrationCheck(coreUri:           RenkuCoreUri,
                        projectGitHttpUrl: projects.GitHttpUrl,
                        accessToken:       AccessToken
  ):               F[Result[ProjectMigrationCheck]]
  def getVersions: F[Result[List[SchemaVersion]]]
  def postProjectUpdate(coreUri:     RenkuCoreUri.Versioned,
                        updates:     ProjectUpdates,
                        accessToken: UserAccessToken
  ): F[Result[Branch]]
}

private object LowLevelApis {
  def apply[F[_]: Async: Logger](coreLatestUri: RenkuCoreUri.Latest): LowLevelApis[F] =
    new LowLevelApisImpl[F](coreLatestUri, ClientTools[F])
}

private class LowLevelApisImpl[F[_]: Async: Logger](coreLatestUri: RenkuCoreUri.Latest, clientTools: ClientTools[F])
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with LowLevelApis[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  import clientTools._

  override def getApiVersion(uri: RenkuCoreUri.ForSchema): F[Result[SchemaApiVersions]] =
    send(GET(uri.uri / "renku" / "apiversion")) {
      case (Ok, _, resp) =>
        toResult[SchemaApiVersions](resp)
      case reqInfo @ (NotFound, _, _) =>
        toFailure[SchemaApiVersions](s"Api version info for ${uri.uri} does not exist")(reqInfo)
      case reqInfo =>
        toFailure[SchemaApiVersions](s"Finding api version info for ${uri.uri} failed")(reqInfo)
    }

  override def getMigrationCheck(coreUri:           RenkuCoreUri,
                                 projectGitHttpUrl: projects.GitHttpUrl,
                                 accessToken:       AccessToken
  ): F[Result[ProjectMigrationCheck]] = {

    val uri = (coreUri.uri / "renku" / "cache.migrations_check")
      .withQueryParam("git_url", projectGitHttpUrl.value)

    send(GET(uri).withHeaders(Header.Raw(ci"gitlab-token", accessToken.value))) {
      case (Ok, _, resp) =>
        toResult[ProjectMigrationCheck](resp)
      case reqInfo =>
        toFailure[ProjectMigrationCheck](s"Migration check for $projectGitHttpUrl failed")(reqInfo)
    }
  }

  override def getVersions: F[Result[List[SchemaVersion]]] = {
    val decoder = Decoder.instance[List[SchemaVersion]] { res =>
      val singleVersionDecoder =
        Decoder.instance(_.downField("data").downField("metadata_version").as[SchemaVersion])

      res.downField("versions").as(decodeList(singleVersionDecoder))
    }

    send(GET(coreLatestUri.uri / "renku" / "versions")) {
      case (Ok, _, resp) => toResult[List[SchemaVersion]](resp)(decoder)
      case reqInfo       => toFailure[List[SchemaVersion]]("Version info cannot be found")(reqInfo)
    }
  }

  override def postProjectUpdate(uri:         RenkuCoreUri.Versioned,
                                 updates:     ProjectUpdates,
                                 accessToken: UserAccessToken
  ): F[Result[Branch]] =
    send(
      request(POST, uri.uri / "renku" / uri.apiVersion / "project.edit", accessToken)
        .withEntity(updates.asJson)
        .putHeaders(Header.Raw(ci"renku-user-email", updates.userInfo.email.value))
        .putHeaders(Header.Raw(ci"renku-user-fullname", updates.userInfo.name.value))
    ) {
      case (Ok, _, resp) => toResult[Branch](resp)(toSuccessfulEdit)
      case reqInfo       => toFailure[Branch]("Submitting Project Edit payload failed")(reqInfo)
    }

  private lazy val toSuccessfulEdit: Decoder[Branch] = Decoder.instance { cur =>
    def failure[A](message: Option[String] = None) = {
      val m = message.fold("")(v => s": $v")
      DecodingFailure(CustomReason(s"Submitting Project Edit payload did not succeed$m"), cur).asLeft[A]
    }

    cur.downField("edited").success.fold(failure[Unit]())(_ => ().asRight[DecodingFailure]) >>
      cur.downField("remote_branch").as[Option[Branch]].flatMap {
        case None    => failure[Branch]("no info about branch".some)
        case Some(b) => b.asRight
      }
  }
}