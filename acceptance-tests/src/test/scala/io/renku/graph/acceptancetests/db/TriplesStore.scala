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

package io.renku.graph.acceptancetests.db

import cats.effect.{IO, Temporal}
import cats.{Applicative, Monad}
import eu.timepit.refined.auto._
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.projectauth.{ProjectAuthData, QueryFilter}
import io.renku.triplesstore._
import io.renku.triplesstore.client.util.JenaRunMode
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

object TriplesStore extends InMemoryJena with ProjectsDataset with MigrationsDataset {

  protected override val jenaRunMode: JenaRunMode = JenaRunMode.FixedPortContainer(3030)

  def start()(implicit logger: Logger[IO]): IO[Unit] = for {
    _ <- Applicative[IO].unlessA(isRunning)(IO(container.start()))
    _ <- waitForReadiness
    _ <- logger.info("Triples Store started")
  } yield ()

  private def isRunning: Boolean = Option(container.containerInfo).exists(_.getState.getRunning)

  private def waitForReadiness(implicit logger: Logger[IO]): IO[Unit] =
    Monad[IO].whileM_(IO(!isRunning))(logger.info("Waiting for TS") >> (Temporal[IO] sleep (500 millis)))

  def findProjectAuth(
      slug: projects.Slug
  )(implicit renkuUrl: RenkuUrl, sqtr: SparqlQueryTimeRecorder[IO], L: Logger[IO]): IO[Option[ProjectAuthData]] =
    ProjectSparqlClient[IO](projectsDSConnectionInfo)
      .map(_.asProjectAuthService)
      .use(_.getAll(QueryFilter.all.withSlug(slug)).compile.last)
}
