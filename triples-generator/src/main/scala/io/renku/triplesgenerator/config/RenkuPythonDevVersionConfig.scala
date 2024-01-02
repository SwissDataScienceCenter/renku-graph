/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import cats.{MonadThrow, Show}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader.Result
import pureconfig.error.ConfigReaderException
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource, ReadsMissingKeys}

final case class RenkuPythonDevVersion(version: String)
object RenkuPythonDevVersion {
  implicit lazy val show: Show[RenkuPythonDevVersion] = Show.show(_.version)
}

object RenkuPythonDevVersionConfig {

  private val optionalStringReader: ConfigReader[Option[String]] =
    new ConfigReader[Option[String]] with ReadsMissingKeys {
      override def from(cur: ConfigCursor): Result[Option[String]] =
        if (cur.isUndefined)
          Right(Option.empty[String])
        else
          ConfigReader[Option[String]].from(cur) >>= {
            case None                                  => None.asRight
            case Some(version) if version.trim.isEmpty => None.asRight
            case Some(version)                         => version.trim.some.asRight
          }
    }

  private implicit val reader: ConfigReader[Option[RenkuPythonDevVersion]] =
    ConfigReader.forProduct1[Option[RenkuPythonDevVersion], Option[String]]("renku-python-dev-version") {
      _.map(RenkuPythonDevVersion(_))
    }(optionalStringReader)

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load): F[Option[RenkuPythonDevVersion]] =
    MonadThrow[F].fromEither(
      ConfigSource
        .fromConfig(config)
        .load[Option[RenkuPythonDevVersion]]
        .leftMap(ConfigReaderException(_))
    )
}
