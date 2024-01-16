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
package projectslug

import cats.effect.{Async, Ref}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.Schemas._
import io.renku.graph.model.{GraphClass, RenkuUrl, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Triple
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger
import tooling.RecordsFinder

private trait BacklogCreator[F[_]] {
  def createBacklog(): F[Unit]
}

private object BacklogCreator {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[BacklogCreator[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    recordsFinder           <- ProjectsConnectionConfig.fromConfig[F]().map(RecordsFinder[F](_))
    migrationsDSClient      <- MigrationsConnectionConfig.fromConfig[F]().map(TSClient[F](_))
  } yield new BacklogCreatorImpl[F](recordsFinder, migrationsDSClient)

  def asToBeMigratedInserts(implicit ru: RenkuUrl): List[projects.Slug] => Option[SparqlQuery] =
    toTriples andThen toInsertQuery

  private def toTriples(implicit ru: RenkuUrl): List[projects.Slug] => List[Triple] =
    _.map(slug => Triple(AddProjectSlug.name.asEntityId, renku / "toBeMigrated", slug.asObject))

  private lazy val toInsertQuery: List[Triple] => Option[SparqlQuery] = {
    case Nil => None
    case triples =>
      SparqlQuery
        .ofUnsafe(
          show"${AddProjectSlug.name} - store to backlog",
          sparql"INSERT DATA {\n${triples.map(_.asSparql).combineAll}\n}"
        )
        .some
  }
}

private class BacklogCreatorImpl[F[_]: Async](recordsFinder: RecordsFinder[F], migrationsDSClient: TSClient[F])(implicit
    ru: RenkuUrl
) extends BacklogCreator[F] {

  import BacklogCreator._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import io.renku.triplesstore.ResultsDecoder._
  import recordsFinder._

  private val pageSize: Int = 50

  override def createBacklog(): F[Unit] = addPageToBacklog(Ref.unsafe(1))

  private def addPageToBacklog(currentPage: Ref[F, Int]): F[Unit] =
    currentPage
      .getAndUpdate(_ + 1)
      .map(query)
      .flatMap(findRecords[projects.Slug])
      .map(asToBeMigratedInserts)
      .flatMap(storeInBacklog(currentPage))

  private def query(page: Int) = SparqlQuery.ofUnsafe(
    show"${AddProjectSlug.name} - projects to migrate",
    Prefixes of (renku -> "renku", schema -> "schema"),
    sparql"""|SELECT DISTINCT ?slug
             |WHERE {
             |  GRAPH ?id {
             |    ?id a schema:Project.
             |  }
             |  {
             |    GRAPH ${GraphClass.Projects.id} {
             |      ?id renku:projectPath ?slug
             |      FILTER NOT EXISTS {
             |        ?id renku:slug ?slg
             |      }
             |    }
             |  } UNION {
             |    GRAPH ?id {
             |      ?id renku:projectPath ?slug
             |      FILTER NOT EXISTS {
             |        ?id renku:slug ?slg
             |      }
             |    }
             |  }
             |}
             |ORDER BY ?slug
             |LIMIT $pageSize
             |OFFSET ${(page - 1) * pageSize}
             |""".stripMargin
  )

  private implicit lazy val decoder: Decoder[List[projects.Slug]] = ResultsDecoder[List, projects.Slug] {
    implicit cur => extract[projects.Slug]("slug")
  }

  private def storeInBacklog(currentPage: Ref[F, Int]): Option[SparqlQuery] => F[Unit] = {
    case None        => ().pure[F]
    case Some(query) => migrationsDSClient.updateWithNoResult(query) >> addPageToBacklog(currentPage)
  }
}
