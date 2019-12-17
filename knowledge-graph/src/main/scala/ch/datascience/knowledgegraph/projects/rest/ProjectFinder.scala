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

package ch.datascience.knowledgegraph.projects.rest

import cats.MonadError
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.knowledgegraph.projects.model.Project

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class ProjectFinder[Interpretation[_]](kgMetadataFinder: KGMetadataFinder[Interpretation]) {

  def findProject(projectPath: ProjectPath): Interpretation[Option[Project]] =
    kgMetadataFinder.findProject(projectPath)

}

private object IOProjectFinder {
  import cats.effect.{ContextShift, IO, Timer}
  import com.typesafe.config.{Config, ConfigFactory}
  import io.chrisdavenport.log4cats.Logger

  def apply(
      logger:         Logger[IO],
      config:         Config = ConfigFactory.load()
  )(implicit ME:      MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]): IO[ProjectFinder[IO]] =
    for {
      metadataFinder <- IOKGMetadataFinder(logger = logger)
    } yield new ProjectFinder[IO](metadataFinder)
}
