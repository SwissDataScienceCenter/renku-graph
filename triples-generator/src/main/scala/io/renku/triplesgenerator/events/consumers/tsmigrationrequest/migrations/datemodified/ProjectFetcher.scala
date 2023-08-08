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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.datemodified

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait ProjectFetcher[F[_]] {
  def fetchProject(slug: projects.Slug): F[Option[ProjectInfo]]
}

private object ProjectFetcher {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectFetcher[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new ProjectFetcherImpl[F](_))
}

private class ProjectFetcherImpl[F[_]: Async](tsClient: TSClient[F]) extends ProjectFetcher[F] {

  override def fetchProject(slug: projects.Slug): F[Option[ProjectInfo]] = tsClient.queryExpecting[Option[ProjectInfo]](
    SparqlQuery.ofUnsafe(
      show"${AddProjectDateModified.name} - find projects",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?id ?slug ?dateCreated
               |WHERE {
               |  BIND (${slug.asObject} AS ?slug)
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:projectPath ?slug;
               |        schema:dateCreated ?dateCreated.
               |  }
               |}
               |GROUP BY ?id ?slug ?dateCreated
               |LIMIT 1
               |""".stripMargin
    )
  )

  private implicit lazy val decoder: Decoder[Option[ProjectInfo]] = ResultsDecoder[Option, ProjectInfo] {
    implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (
        extract[projects.ResourceId]("id"),
        extract[projects.Slug]("slug"),
        extract[projects.DateCreated]("dateCreated")
      ).mapN(ProjectInfo)
  }
}
