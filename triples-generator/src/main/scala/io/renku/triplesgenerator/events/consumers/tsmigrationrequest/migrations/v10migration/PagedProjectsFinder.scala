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
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.{Schemas, projects}
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecordsFinder
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait PagedProjectsFinder[F[_]] {
  def findProjects(page: Int): F[List[projects.Path]]
}

private object PagedProjectsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[PagedProjectsFinder[F]] =
    ProjectsConnectionConfig[F]().map(RecordsFinder[F](_)).map(new PagedProjectsFinderImpl(_))
}

private class PagedProjectsFinderImpl[F[_]](recordsFinder: RecordsFinder[F])
    extends PagedProjectsFinder[F]
    with Schemas {

  import ResultsDecoder._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  private val chunkSize: Int = 50

  override def findProjects(page: Int): F[List[projects.Path]] =
    recordsFinder.findRecords(query(page))

  private def query(page: Int) = SparqlQuery.of(
    MigrationToV10.name.asRefined,
    Prefixes of (schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?path
        |WHERE {
        |  GRAPH ?id {
        |    ?id a schema:Project;
        |        renku:projectPath ?path
        |  }
        |}
        |ORDER BY ?path
        |LIMIT $chunkSize
        |OFFSET ${(page - 1) * chunkSize}
        |""".stripMargin
  )

  private implicit lazy val decoder: Decoder[List[projects.Path]] = ResultsDecoder[List, projects.Path] {
    implicit cur => extract[projects.Path]("path")
  }
}
