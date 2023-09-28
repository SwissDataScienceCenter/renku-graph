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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.lucenereindex

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.triplesstore.{MigrationsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait ProjectDonePersister[F[_]] {
  def noteDone(slug: projects.Slug): F[Unit]
}

private object ProjectDonePersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectDonePersister[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    tsClient                <- MigrationsConnectionConfig[F]().map(TSClient[F](_))
  } yield new ProjectDonePersisterImpl[F](tsClient)
}

private class ProjectDonePersisterImpl[F[_]](tsClient: TSClient[F])(implicit ru: RenkuUrl)
    extends ProjectDonePersister[F] {

  import io.renku.graph.model.Schemas.renku
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.client.model.Triple
  import io.renku.triplesstore.client.syntax._

  override def noteDone(slug: projects.Slug): F[Unit] =
    tsClient.updateWithNoResult(v10MigratedTriple(slug))

  private def v10MigratedTriple(slug: projects.Slug) = {
    val triple = Triple(ReindexLucene.name.asEntityId, renku / "toBeMigrated", slug.asObject)
    SparqlQuery.ofUnsafe(
      show"${ReindexLucene.name} - store migrated",
      s"DELETE DATA {${triple.asSparql.sparql}}"
    )
  }
}
