/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.init

import cats.MonadError
import io.renku.graph.model.CliVersion
import pureconfig.ConfigReader

private[init] object CliVersionLoader {

  import io.renku.config.ConfigLoader._

  private implicit val cliVersionLoader: ConfigReader[CliVersion] = stringTinyTypeReader(CliVersion)

  def apply[F[_]]()(implicit ME: MonadError[F, Throwable]): F[CliVersion] = apply(findRenkuVersion)

  private[init] def apply[F[_]](renkuVersionFinder: F[CliVersion])(implicit
      ME:                                           MonadError[F, Throwable]
  ): F[CliVersion] = renkuVersionFinder

  private def findRenkuVersion[F[_]](implicit ME: MonadError[F, Throwable]): F[CliVersion] = {
    import ammonite.ops._
    import cats.syntax.all._

    for {
      versionAsString <- ME.catchNonFatal(%%("renku", "--version")(pwd).out.string.trim)
      version         <- ME.fromEither(CliVersion.from(versionAsString))
    } yield version
  }
}
