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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import fs2.Stream
import io.circe.Decoder
import io.renku.graph.model.Schemas
import io.renku.graph.model.projects.{Path => ProjectPath}
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

trait AllProjects[F[_]] {
  def findAll(chunkSize: Int): Stream[F, AllProjects.ProjectMetadata]
}

object AllProjects {
  def create[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[AllProjects[F]] =
    ProjectsConnectionConfig[F]().map(apply[F])

  final case class ProjectMetadata(path: ProjectPath)

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      tsConfig: DatasetConnectionConfig
  ): AllProjects[F] =
    new TSClientImpl[F](tsConfig) with AllProjects[F] {
      def findAll(chunkSize: Int): Stream[F, AllProjects.ProjectMetadata] =
        findFrom(0, chunkSize)

      def findFrom(offset: Int, chunkSize: Int): Stream[F, ProjectMetadata] =
        loadAllChunks(offset, chunkSize).takeWhile(_.nonEmpty).flatMap(Stream.emits(_))

      def loadAllChunks(offset: Int, chunkSize: Int): Stream[F, List[ProjectMetadata]] =
        Stream.eval(loadChunk(offset, chunkSize)) ++ loadAllChunks(offset + chunkSize, chunkSize)

      def loadChunk(offset: Int, chunkSize: Int): F[List[ProjectMetadata]] =
        queryExpecting[List[ProjectMetadata]](projectsQuery(offset, chunkSize))

      def projectsQuery(offset: Int, limit: Int): SparqlQuery =
        SparqlQuery.of(
          "TS migration: find projects",
          Prefixes.of(Schemas.schema -> "schema", Schemas.renku -> "renku"),
          s"""
             |SELECT DISTINCT ?projectPath
             |WHERE {
             |  Graph ?g {
             |    ?projectId a schema:Project;
             |      renku:projectPath ?projectPath.
             |  }
             |}
             |ORDER BY ?projectId
             |OFFSET $offset
             |LIMIT $limit
             |""".stripMargin
        )

      implicit val decoder: Decoder[List[ProjectMetadata]] =
        ResultsDecoder[List, ProjectMetadata] { implicit cursor =>
          for {
            path <- extract[ProjectPath]("projectPath")
          } yield ProjectMetadata(path)
        }
    }
}
