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

package io.renku.graph.acceptancetests.db

import cats.effect.{IO, Temporal}
import cats.syntax.all._
import cats.{Applicative, Monad}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.graph.acceptancetests.data.RdfStoreData.currentVersionPair
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuVersionPair
import io.renku.graph.model.Schemas.renku
import io.renku.jsonld.EntityId
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

object TriplesStore extends InMemoryJena with RenkuDataset with MigrationsDataset {

  protected override val maybeJenaFixedPort: Option[Int Refined Positive] = Some(3030)

  def start(
      maybeVersionPair: Option[RenkuVersionPair] = Some(currentVersionPair)
  )(implicit logger:    Logger[IO]): IO[Unit] = for {
    _ <- Applicative[IO].unlessA(isRunning)(IO(container.start()))
    _ <- waitForReadiness
    _ <- logger.info("Triples Store started")
    _ <- createDatasets()
    _ <- logger.info("Triples Store datasets created")
    _ <- updateVersionPair(maybeVersionPair)
  } yield ()

  private def isRunning: Boolean = Option(container.containerInfo).exists(_.getState.getRunning)

  private def waitForReadiness(implicit logger: Logger[IO]): IO[Unit] =
    Monad[IO].whileM_(IO(!isRunning))(logger.info("Waiting for TS") >> (Temporal[IO] sleep (500 millis)))

  private def updateVersionPair(maybeVersionPair: Option[RenkuVersionPair])(implicit logger: Logger[IO]): IO[Unit] =
    maybeVersionPair match {
      case None => IO.unit
      case Some(versionPair) =>
        for {
          renkuUrl <- RenkuUrlLoader[IO]()
          _ <- runUpdate(
                 on = migrationsDataset,
                 SparqlQuery.of(
                   "version-pair insert",
                   s"""|INSERT DATA {
                       |<${EntityId.of((renkuUrl / "version-pair").toString)}> a <${renku / "VersionPair"}> ;
                       |                                                       <${renku / "cliVersion"}> '${versionPair.cliVersion}' ;
                       |                                                       <${renku / "schemaVersion"}> '${versionPair.schemaVersion}'}""".stripMargin
                 )
               )
          _ <- logger.info(show"Triples Store on $versionPair")
        } yield ()
    }
}
