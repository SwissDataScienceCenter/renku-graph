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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package lucenereindex

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.Schemas._
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Triple
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger
import tooling.AllProjects

private trait BacklogCreator[F[_]] {
  def createBacklog(): F[Unit]
}

private[lucenereindex] object BacklogCreator {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](migrationName: Migration.Name): F[BacklogCreator[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    allProjects             <- ProjectsConnectionConfig[F]().map(AllProjects[F](_))
    migrationsDSClient      <- MigrationsConnectionConfig[F]().map(TSClient[F](_))
  } yield new BacklogCreatorImpl[F](migrationName, allProjects, migrationsDSClient)

  def asToBeMigratedInserts(migrationName: Migration.Name, slug: projects.Slug)(implicit ru: RenkuUrl): SparqlQuery =
    toInsertQuery(migrationName, toTriples(migrationName, slug))

  private def toTriples(migrationName: Migration.Name, slug: projects.Slug)(implicit ru: RenkuUrl): Triple =
    Triple(migrationName.asEntityId, renku / "toBeMigrated", slug.asObject)

  private def toInsertQuery(migrationName: Migration.Name, triple: Triple): SparqlQuery =
    SparqlQuery
      .ofUnsafe(
        show"$migrationName - store to backlog",
        sparql"INSERT DATA {$triple}"
      )
}

private class BacklogCreatorImpl[F[_]: Async](migrationName: Migration.Name,
                                              allProjects:        AllProjects[F],
                                              migrationsDSClient: TSClient[F]
)(implicit
    ru: RenkuUrl
) extends BacklogCreator[F] {

  import BacklogCreator._

  private val pageSize: Int = 50

  override def createBacklog(): F[Unit] =
    allProjects
      .findAll(pageSize)
      .map(md => asToBeMigratedInserts(migrationName, md.slug))
      .evalMap(migrationsDSClient.updateWithNoResult)
      .compile
      .drain
}
