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

package io.renku.triplesgenerator.config

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.model.{CliVersion, RenkuVersionPair, SchemaVersion}
import org.typelevel.log4cats.Logger
import pureconfig.ConfigReader

import scala.util.control.NonFatal
trait VersionCompatibilityConfig

private object VersionCompatibilityConfig extends VersionCompatibilityConfig {

  import cats.syntax.all._
  import io.renku.config.ConfigLoader._

  private val separator = "->"

  implicit val reader = ConfigReader[List[String]].map(_.map { pair =>
    val (cliVersion, schemaVersion): (String, String) = pair.split(separator).toList match {
      case List(cliVersion, schemaVersion) => (cliVersion, schemaVersion)
      case _                               => throw new Exception(s"Did not find exactly two elements: ${pair}")
    }
    RenkuVersionPair(CliVersion(cliVersion.trim), SchemaVersion(schemaVersion.trim))
  })

  def apply[Interpretation[_]](
      maybeRenkuDevVersion: Option[RenkuPythonDevVersion],
      logger:               Logger[Interpretation],
      config:               Config
  )(implicit ME:            MonadError[Interpretation, Throwable]): Interpretation[NonEmptyList[RenkuVersionPair]] =
    find[Interpretation, List[RenkuVersionPair]]("compatibility-matrix", config)(reader, ME).flatMap {
      case Nil =>
        ME.raiseError[NonEmptyList[RenkuVersionPair]](
          new Exception("No compatibility matrix provided for schema version")
        )
      case head :: tail =>
        maybeRenkuDevVersion match {
          case Some(devVersion) =>
            logger
              .warn(
                s"RENKU_PYTHON_DEV_VERSION env variable is set. CLI config version is now set to ${devVersion.version}"
              )
              .map(_ => NonEmptyList(head.copy(cliVersion = CliVersion(devVersion.version)), tail))
          case None => NonEmptyList(head, tail).pure[Interpretation]
        }
    }
}

object IOVersionCompatibilityConfig {
  def apply(logger: Logger[IO], config: Config = ConfigFactory.load): IO[NonEmptyList[RenkuVersionPair]] = for {
    maybeRenkuDevVersion       <- RenkuPythonDevVersionConfig[IO]() recoverWith errorToNone
    versionCompatibilityConfig <- VersionCompatibilityConfig(maybeRenkuDevVersion, logger, config)
  } yield versionCompatibilityConfig

  private lazy val errorToNone: PartialFunction[Throwable, IO[Option[RenkuPythonDevVersion]]] = { case NonFatal(_) =>
    None.pure[IO]
  }
}
