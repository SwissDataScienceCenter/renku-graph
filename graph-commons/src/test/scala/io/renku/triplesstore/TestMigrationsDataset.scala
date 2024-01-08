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

package io.renku.triplesstore

import TestDatasetCreation._
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.renku.graph.triplesstore.DatasetTTLs.MigrationsTTL
import io.renku.triplesstore.client.http.SparqlClient
import org.typelevel.log4cats.Logger

trait TestMigrationsDataset extends TestDataset {
  self: GraphJenaSpec =>

  def migrationsDSConfig(implicit L: Logger[IO]): Resource[IO, MigrationsConnectionConfig] =
    ttlResource >>= { ttl =>
      (clientResource >>= datasetResource(ttl))
        .as(dsConnectionConfig(ttl, server.conConfig, MigrationsConnectionConfig.apply))
    }

  def migrationsDSResource(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    migrationsDSConfig
      .flatMap(conf => SparqlClient[IO](conf.toCC()))

  private lazy val ttlResource =
    loadTtl(MigrationsTTL)
      .flatMap(ttl => generateName(ttl, getClass).tupleLeft(ttl))
      .map { case (origTtl, newName) => updateDSConfig(origTtl, newName, new MigrationsTTL(_, _)) }
      .toResource
}
