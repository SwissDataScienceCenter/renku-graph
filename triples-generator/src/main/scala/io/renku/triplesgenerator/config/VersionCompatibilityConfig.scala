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

package io.renku.triplesgenerator.config

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.model.{CliVersion, RenkuVersionPair, SchemaVersion}
import org.typelevel.log4cats.Logger
import pureconfig.ConfigReader

import scala.util.control.NonFatal

private object RenkuVersionPairsReader {

  import cats.syntax.all._
  import io.renku.config.ConfigLoader._

  private val separator = "->"

  private implicit val reader: ConfigReader[List[RenkuVersionPair]] = ConfigReader[List[String]].map(_.map { pair =>
    val (cliVersion, schemaVersion): (String, String) = pair.split(separator).toList match {
      case List(cliVersion, schemaVersion) => (cliVersion, schemaVersion)
      case _                               => throw new Exception(s"Did not find exactly two elements: ${pair}")
    }
    RenkuVersionPair(CliVersion(cliVersion.trim), SchemaVersion(schemaVersion.trim))
  })

  def readRenkuVersionPairs[F[_]: MonadThrow: Logger](
      maybeRenkuDevVersion: Option[RenkuPythonDevVersion],
      config:               Config
  ): F[NonEmptyList[RenkuVersionPair]] =
    find[F, List[RenkuVersionPair]]("compatibility-matrix", config).flatMap {
      case Nil =>
        new Exception("No compatibility matrix provided for schema version")
          .raiseError[F, NonEmptyList[RenkuVersionPair]]
      case head :: tail =>
        maybeRenkuDevVersion match {
          case Some(devVersion) =>
            Logger[F]
              .warn(
                s"RENKU_PYTHON_DEV_VERSION env variable is set. CLI config version is now set to ${devVersion.version}"
              )
              .map(_ => NonEmptyList(head.copy(cliVersion = CliVersion(devVersion.version)), tail))
          case None => NonEmptyList(head, tail).pure[F]
        }
    }
}

object VersionCompatibilityConfig {

  def apply[F[_]: MonadThrow: Logger](config: Config = ConfigFactory.load): F[NonEmptyList[RenkuVersionPair]] = for {
    maybeRenkuDevVersion       <- RenkuPythonDevVersionConfig[F]() recoverWith errorToNone
    versionCompatibilityConfig <- RenkuVersionPairsReader.readRenkuVersionPairs[F](maybeRenkuDevVersion, config)
  } yield versionCompatibilityConfig

  private def errorToNone[F[_]: MonadThrow]: PartialFunction[Throwable, F[Option[RenkuPythonDevVersion]]] = {
    case NonFatal(_) => Option.empty[RenkuPythonDevVersion].pure[F]
  }
}
