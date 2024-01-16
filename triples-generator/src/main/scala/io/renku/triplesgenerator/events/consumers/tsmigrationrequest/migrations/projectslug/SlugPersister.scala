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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.projectslug

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.GraphClass
import io.renku.graph.model.Schemas.renku
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait SlugPersister[F[_]] {
  def persistSlug(projectInfo: ProjectInfo): F[Unit]
}

private object SlugPersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): SlugPersister[F] = new SlugPersisterImpl[F](TSClient[F](connectionConfig))
}

private class SlugPersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends SlugPersister[F] {

  override def persistSlug(projectInfo: ProjectInfo): F[Unit] =
    insertSlug(projectInfo)

  private lazy val insertSlug: ProjectInfo => F[Unit] = { case ProjectInfo(id, slug) =>
    tsClient.updateWithNoResult(
      SparqlQuery.ofUnsafe(
        show"${AddProjectSlug.name} - insert slug",
        Prefixes of renku -> "renku",
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Projects.id} { ${id.asEntityId} renku:slug ${slug.asObject} }
                 |  GRAPH ${id.asEntityId} { ${id.asEntityId} renku:slug ${slug.asObject} }
                 |}
                 |""".stripMargin
      )
    )
  }
}
