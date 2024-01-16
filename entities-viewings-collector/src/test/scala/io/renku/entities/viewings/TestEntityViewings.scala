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

package io.renku.entities.viewings

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent}
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig}
import org.typelevel.log4cats.Logger

trait TestEntityViewings {
  self: GraphJenaSpec with AsyncIOSpec =>

  def provision(event: ProjectViewedEvent)(implicit pcc: ProjectsConnectionConfig, L: Logger[IO]): IO[Unit] =
    projectViewedEventPersister.persist(event)

  def provision(event: DatasetViewedEvent)(implicit pcc: ProjectsConnectionConfig, L: Logger[IO]): IO[Unit] =
    datasetViewedEventUploader.upload(event)

  private def projectViewedEventPersister(implicit pcc: ProjectsConnectionConfig, L: Logger[IO]) =
    collector.projects.viewed.EventPersister[IO](tsClient)
  private def datasetViewedEventUploader(implicit pcc: ProjectsConnectionConfig, L: Logger[IO]) =
    collector.datasets.EventUploader[IO](tsClient)
}
