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

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Decoder
import io.renku.config.ConfigLoader.find
import io.renku.tinytypes.constraints.NonNegativeInt
import io.renku.tinytypes.json.TinyTypeDecoders
import io.renku.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}
import pureconfig.ConfigReader

import java.time.Period

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

private final class AccessTokenId private (val value: Int) extends AnyVal with IntTinyType
private object AccessTokenId
    extends TinyTypeFactory[AccessTokenId](new AccessTokenId(_))
    with NonNegativeInt[AccessTokenId] {
  implicit val jsonDecoder: Decoder[AccessTokenId] = TinyTypeDecoders.intDecoder(this)
}
