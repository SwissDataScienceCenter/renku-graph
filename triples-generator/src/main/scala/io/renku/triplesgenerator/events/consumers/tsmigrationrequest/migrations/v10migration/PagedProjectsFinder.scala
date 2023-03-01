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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package v10migration

import cats.Monad
import cats.effect.{Async, Ref}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.{Schemas, projects}
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger
import tooling.RecordsFinder

private trait PagedProjectsFinder[F[_]] {
  def nextProjectsPage(): F[List[projects.Path]]
}

private object PagedProjectsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[PagedProjectsFinder[F]] = for {
    recordsFinder           <- ProjectsConnectionConfig[F]().map(RecordsFinder[F](_))
    startTimeFinder         <- MigrationStartTimeFinder[F]
    migratedProjectsChecker <- MigratedProjectsChecker[F]
    initialPage             <- Ref.of(1)
  } yield new PagedProjectsFinderImpl(recordsFinder, startTimeFinder, migratedProjectsChecker, initialPage)
}

private class PagedProjectsFinderImpl[F[_]: Monad](recordsFinder: RecordsFinder[F],
                                                   migrationDateFinder:     MigrationStartTimeFinder[F],
                                                   migratedProjectsChecker: MigratedProjectsChecker[F],
                                                   currentPage:             Ref[F, Int]
) extends PagedProjectsFinder[F]
    with Schemas {

  import ResultsDecoder._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import io.renku.triplesstore.client.syntax._
  import migrationDateFinder._
  import recordsFinder._

  import java.time.Instant

  private val pageSize: Int = 50

  override def nextProjectsPage(): F[List[projects.Path]] =
    (findMigrationStartDate -> currentPage.getAndUpdate(_ + 1))
      .mapN(query)
      .flatMap(findRecords[projects.Path])
      .flatMap(filterNotMigrated)

  private def query(startDate: Instant, page: Int) = SparqlQuery.ofUnsafe(
    show"${MigrationToV10.name} - find projects",
    Prefixes of (schema -> "schema", renku -> "renku", xsd -> "xsd"),
    s"""|SELECT DISTINCT ?path
        |WHERE {
        |  GRAPH ?id {
        |    ?id a schema:Project;
        |        renku:projectPath ?path;
        |        schema:dateCreated ?created.
        |    FILTER (?created <= ${startDate.asTripleObject.asSparql.sparql})
        |  }
        |}
        |ORDER BY ?path
        |LIMIT $pageSize
        |OFFSET ${(page - 1) * pageSize}
        |""".stripMargin
  )

  private implicit lazy val decoder: Decoder[List[projects.Path]] = ResultsDecoder[List, projects.Path] {
    implicit cur => extract[projects.Path]("path")
  }

  private lazy val filterNotMigrated: List[projects.Path] => F[List[projects.Path]] = {
    case Nil => List.empty[projects.Path].pure[F]
    case nonEmpty =>
      migratedProjectsChecker
        .filterNotMigrated(nonEmpty)
        .flatMap {
          case Nil      => nextProjectsPage()
          case nonEmpty => nonEmpty.pure[F]
        }
  }
}
