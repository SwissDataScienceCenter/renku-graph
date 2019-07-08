/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.IORdfStoreClient.{Query, RdfUpdate}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import ch.datascience.triplesgenerator.config.SchemaVersion
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait SchemaVersionUpdater[Interpretation[_]] {
  def updateSchemaVersion(projectPath: ProjectPath): Interpretation[Unit]
}

private class IOSchemaVersionUpdater(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    schemaVersion:           SchemaVersion,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfUpdate](rdfStoreConfig, logger)
    with SchemaVersionUpdater[IO] {

  override def updateSchemaVersion(projectPath: ProjectPath): IO[Unit] =
    send {
      Query {
        s"""
           |PREFIX dcterms: <http://purl.org/dc/terms/>
           |
           |DELETE {<${renkuBaseUrl / projectPath}> dcterms:hasVersion ?version } 
           |INSERT {<${renkuBaseUrl / projectPath}> dcterms:hasVersion "$schemaVersion"} 
           |WHERE { 
           |  OPTIONAL {
           |    ?project dcterms:hasVersion ?version .
           |  }
           |}""".stripMargin
      }
    }(unitResponseMapper)
}

private object IOSchemaVersionUpdater {
  def apply(
      schemaVersion:           IO[SchemaVersion],
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[SchemaVersionUpdater[IO]] =
    for {
      config        <- rdfStoreConfig
      renkuBaseUrl  <- renkuBaseUrl
      schemaVersion <- schemaVersion
    } yield new IOSchemaVersionUpdater(config, renkuBaseUrl, schemaVersion, logger)
}
