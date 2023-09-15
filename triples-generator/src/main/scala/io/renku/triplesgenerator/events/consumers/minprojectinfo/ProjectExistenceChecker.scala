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

package io.renku.triplesgenerator.events.consumers.minprojectinfo

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait ProjectExistenceChecker[F[_]] {
  def checkProjectExists(projectId: projects.ResourceId): F[Boolean]
}

private object ProjectExistenceChecker {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectExistenceChecker[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new ProjectExistenceCheckerImpl[F](_))
}

private class ProjectExistenceCheckerImpl[F[_]](tsClient: TSClient[F]) extends ProjectExistenceChecker[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas.schema
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import io.renku.triplesstore.SparqlQuery.Prefixes

  override def checkProjectExists(projectId: projects.ResourceId): F[Boolean] = tsClient.queryExpecting[Boolean] {
    SparqlQuery.ofUnsafe(
      s"${categoryName.value.toLowerCase} - project existence check",
      Prefixes of schema -> "schema",
      sparql"""|SELECT ?id
               |WHERE {
               |  BIND (${projectId.asEntityId} AS ?id)
               |  GRAPH ?id { ?id a schema:Project }
               |}
               |LIMIT 1
               |""".stripMargin
    )
  }(decoder)

  private lazy val decoder: Decoder[Boolean] = ResultsDecoder[List, projects.ResourceId] { implicit cur =>
    import io.renku.tinytypes.json.TinyTypeDecoders._
    extract[projects.ResourceId]("id")
  }.map(_.nonEmpty)
}
