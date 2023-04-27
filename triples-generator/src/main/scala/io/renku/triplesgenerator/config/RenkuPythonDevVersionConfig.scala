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

package io.renku.triplesgenerator.config

import cats.{MonadThrow, Show}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

final case class RenkuPythonDevVersion(version: String)
object RenkuPythonDevVersion {
  implicit lazy val show: Show[RenkuPythonDevVersion] = Show.show(_.version)
}

object RenkuPythonDevVersionConfig {

  import io.renku.config.ConfigLoader._

  implicit val reader: ConfigReader[Option[RenkuPythonDevVersion]] = ConfigReader[Option[String]].map {
    case Some(version) if version.trim.isEmpty => None
    case Some(version)                         => Some(RenkuPythonDevVersion(version.trim))
    case None                                  => None
  }

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load): F[Option[RenkuPythonDevVersion]] =
    find[F, Option[RenkuPythonDevVersion]]("renku-python-dev-version", config)
}
