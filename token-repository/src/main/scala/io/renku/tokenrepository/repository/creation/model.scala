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

package io.renku.tokenrepository.repository
package creation

import AccessTokenCrypto.EncryptedAccessToken
import TokenDates._
import cats.syntax.all._
import cats.{MonadThrow, Show}
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Decoder
import io.renku.config.ConfigLoader.find
import io.renku.graph.model.projects
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{InstantNotInTheFuture, NonNegativeInt}
import io.renku.tinytypes.json.TinyTypeDecoders
import pureconfig.ConfigReader

import java.time.{Instant, LocalDate, Period}

private[repository] final case class TokenCreationInfo(token: ProjectAccessToken, dates: TokenDates)

private[repository] final case class TokenDates(createdAt: CreatedAt, expiryDate: ExpiryDate)

private[repository] object TokenDates {
  final class CreatedAt private (val value: Instant) extends AnyVal with InstantTinyType

  implicit object CreatedAt
      extends TinyTypeFactory[CreatedAt](new CreatedAt(_))
      with InstantNotInTheFuture[CreatedAt]
      with TinyTypeJsonLDOps[CreatedAt]

  final class ExpiryDate private (val value: LocalDate) extends AnyVal with LocalDateTinyType
  implicit object ExpiryDate extends TinyTypeFactory[ExpiryDate](new ExpiryDate(_)) with TinyTypeJsonLDOps[ExpiryDate]
}

private[repository] final case class TokenStoringInfo(project:        Project,
                                                      encryptedToken: EncryptedAccessToken,
                                                      dates:          TokenDates
)

private[repository] final case class Project(id: projects.GitLabId, path: projects.Path)

private[repository] object Project {
  implicit lazy val show: Show[Project] = { case Project(id, path) =>
    s"projectId = $id, projectPath = $path"
  }
}

private final class AccessTokenId private (val value: Int) extends AnyVal with IntTinyType
private object AccessTokenId
    extends TinyTypeFactory[AccessTokenId](new AccessTokenId(_))
    with NonNegativeInt[AccessTokenId] {
  implicit val jsonDecoder: Decoder[AccessTokenId] = TinyTypeDecoders.intDecoder(this)
}

private object ProjectTokenDuePeriod {

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load): F[Period] =
    find[F, Period]("project-token-due-period", config)
}

final class RenkuAccessTokenName private (val value: String) extends StringTinyType
private object RenkuAccessTokenName {

  private implicit val reader: ConfigReader[RenkuAccessTokenName] =
    ConfigReader.fromString(new RenkuAccessTokenName(_).asRight)

  def apply(v: String): RenkuAccessTokenName = new RenkuAccessTokenName(v)

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load): F[RenkuAccessTokenName] =
    find[F, RenkuAccessTokenName]("project-token-name", config)
}
