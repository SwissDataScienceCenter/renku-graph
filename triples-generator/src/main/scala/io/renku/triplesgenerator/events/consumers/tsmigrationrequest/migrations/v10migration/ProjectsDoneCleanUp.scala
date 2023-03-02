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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuUrl
import io.renku.triplesstore.{MigrationsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

private trait ProjectsDoneCleanUp[F[_]] {
  def cleanUp(): F[Unit]
}

private object ProjectsDoneCleanUp {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectsDoneCleanUp[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    tsClient                <- MigrationsConnectionConfig[F]().map(TSClient[F](_))
  } yield new ProjectsDoneCleanUpImpl[F](tsClient)
}

private class ProjectsDoneCleanUpImpl[F[_]](tsClient: TSClient[F])(implicit ru: RenkuUrl)
    extends ProjectsDoneCleanUp[F] {

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas.renku
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.SparqlQuery
  import tsClient._

  override def cleanUp(): F[Unit] = updateWithNoResult(cleanUpQuery)

  private lazy val cleanUpQuery =
    SparqlQuery.ofUnsafe(
      show"${MigrationToV10.name} - clean-up",
      Prefixes of (renku -> "renku"),
      s"""|DELETE { ?s renku:migrated ?o }
          |WHERE {
          |  BIND (${MigrationToV10.name.asEntityId.asSparql.sparql} AS ?s)
          |  ?s renku:migrated ?o
          |}
          |""".stripMargin
    )
}
