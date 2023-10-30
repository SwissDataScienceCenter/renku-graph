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

import cats.syntax.all._
import cats.{MonadThrow, Show}
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.find
import io.renku.graph.model.versions.SchemaVersion
import org.http4s.Uri

sealed trait RenkuCoreUri {
  val uri: Uri
}

object RenkuCoreUri {

  final case class Latest(uri: Uri)                                  extends RenkuCoreUri
  final case class ForSchema(uri: Uri, schemaVersion: SchemaVersion) extends RenkuCoreUri
  final case class Versioned(baseUri: ForSchema, apiVersion: ApiVersion) extends RenkuCoreUri {
    val uri: Uri = baseUri.uri
  }

  object Latest {

    private val key = "services.renku-core-latest.url"

    def loadFromConfig[F[_]: MonadThrow](config: Config = ConfigFactory.load): F[Latest] =
      loadUrlFromConfig(config).map(Latest(_))

    private def loadUrlFromConfig[F[_]: MonadThrow](config: Config): F[Uri] =
      find[F, String](key, config).flatMap(toUri[F](key, _))
  }

  trait ForSchemaLoader {
    def loadFromConfig[F[_]: MonadThrow](schemaVersion: SchemaVersion,
                                         config:        Config = ConfigFactory.load
    ): F[ForSchema]
  }
  object ForSchema extends ForSchemaLoader {

    private val key = "services.renku-core-service-urls"

    override def loadFromConfig[F[_]: MonadThrow](schemaVersion: SchemaVersion,
                                                  config:        Config = ConfigFactory.load
    ): F[ForSchema] =
      loadServiceNamesFromConfig(config)
        .flatMap(serviceNameForSchema(schemaVersion))
        .flatMap(toUri[F](key, _))
        .map(ForSchema(_, schemaVersion))

    private def loadServiceNamesFromConfig[F[_]: MonadThrow](config: Config) =
      find[F, String](key, config).map(_.split(",").toList.map(_.trim))

    private def serviceNameForSchema[F[_]: MonadThrow](schemaVersion: SchemaVersion): List[String] => F[String] =
      _.find(_ contains show"v$schemaVersion")
        .fold(new Exception(show"No renku-core for $schemaVersion in the config").raiseError[F, String])(_.pure[F])
  }

  implicit def show[U <: RenkuCoreUri]: Show[U] = Show.show(_.uri.renderString)

  private def toUri[F[_]: MonadThrow](key: String, uri: String): F[Uri] =
    Uri
      .fromString(uri)
      .fold(
        new Exception(s"'$uri' is not a valid '$key' uri", _).raiseError[F, Uri],
        _.pure[F]
      )
}
