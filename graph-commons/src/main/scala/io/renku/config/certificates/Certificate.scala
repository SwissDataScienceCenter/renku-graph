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

package io.renku.config.certificates

import cats.MonadThrow
import cats.syntax.all._
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

final class Certificate private (val value: String) extends AnyVal with StringTinyType
object Certificate extends TinyTypeFactory[Certificate](new Certificate(_)) with NonBlank[Certificate] {

  import com.typesafe.config.{Config, ConfigFactory}
  import io.renku.config.ConfigLoader.find

  def fromConfig[F[_]: MonadThrow](
      config: Config = ConfigFactory.load()
  ): F[Option[Certificate]] =
    find[F, Option[String]]("client-certificate", config)
      .flatMap(blankToNone(_))
      .flatMap {
        case None => MonadThrow[F].pure(None)
        case Some(certContent) =>
          MonadThrow[F].fromEither {
            Certificate.from(certContent) map Option.apply
          }
      }

  private def blankToNone[F[_]: MonadThrow](maybeBody: Option[String]): F[Option[String]] =
    maybeBody
      .map(_.trim)
      .flatMap {
        case ""       => None
        case nonBlank => Some(nonBlank)
      }
      .pure[F]
}
