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

package io.renku.tokenrepository.repository.creation

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, GitLabClient}

import java.time.{LocalDate, Period}

private[tokenrepository] trait NewTokensCreator[F[_]] {
  def createPersonalAccessToken(projectId: projects.Id, accessToken: AccessToken): OptionT[F, TokenCreationInfo]
}

private[tokenrepository] object NewTokensCreator {

  import io.renku.config.ConfigLoader._

  import scala.concurrent.duration.FiniteDuration

  def apply[F[_]: Async: GitLabClient](config: Config = ConfigFactory.load()): F[NewTokensCreator[F]] =
    find[F, FiniteDuration]("project-token-ttl", config)
      .map(duration => Period.ofDays(duration.toDays.toInt))
      .map(projectTokenTTL => new NewTokensCreatorImpl[F](projectTokenTTL))
}

private class NewTokensCreatorImpl[F[_]: Async: GitLabClient](
    projectTokenTTL: Period,
    currentDate:     () => LocalDate = () => LocalDate.now()
) extends NewTokensCreator[F] {

  import cats.effect._
  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.circe.Decoder.decodeString
  import io.circe.literal._
  import org.http4s.Status.{BadRequest, Created, Forbidden, NotFound}
  import org.http4s.circe.jsonOf
  import org.http4s.implicits._
  import org.http4s.{EntityDecoder, Request, Response, Status}

  override def createPersonalAccessToken(projectId:   projects.Id,
                                         accessToken: AccessToken
  ): OptionT[F, TokenCreationInfo] = OptionT {
    GitLabClient[F].post(uri"projects" / projectId.value / "access_tokens",
                         "create-project-access-token",
                         createPayload()
    )(mapResponse)(accessToken.some)
  }

  private def createPayload() = json"""{
    "name":       $renkuTokenName,
    "scopes":     ["api", "read_repository"],
    "expires_at": ${currentDate() plus projectTokenTTL}
  }"""

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[TokenCreationInfo]]] = {
    case (Created, _, response)                    => response.as[TokenCreationInfo].map(Option.apply)
    case (BadRequest | Forbidden | NotFound, _, _) => Option.empty[TokenCreationInfo].pure[F]
  }

  private implicit lazy val tokenDecoder: EntityDecoder[F, TokenCreationInfo] = {
    val tokenDecoder = decodeString.emap { value =>
      ProjectAccessToken.from(value).leftMap(_.getMessage)
    }

    val infoDecoder: Decoder[TokenCreationInfo] = cursor => {
      import TokenDates._
      import io.renku.tinytypes.json.TinyTypeDecoders._
      for {
        token       <- cursor.downField("token").as(tokenDecoder)
        createdDate <- cursor.downField("created_at").as[CreatedAt]
        expiryDate  <- cursor.downField("expires_at").as[ExpiryDate]
      } yield TokenCreationInfo(token, TokenDates(createdDate, expiryDate))
    }

    jsonOf[F, TokenCreationInfo](Sync[F], infoDecoder)
  }
}
