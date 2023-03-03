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

import cats.effect.{Async, Ref}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{projects, RenkuUrl}
import io.renku.graph.model.Schemas._
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecordsFinder
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.model.Triple
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

import java.time.Instant

private trait BacklogCreator[F[_]] {
  def createBacklog(): F[Unit]
}

private object BacklogCreator {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[BacklogCreator[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    startTimeFinder         <- MigrationStartTimeFinder[F]
    recordsFinder           <- ProjectsConnectionConfig[F]().map(RecordsFinder[F](_))
    migrationsDSClient      <- MigrationsConnectionConfig[F]().map(TSClient[F](_))
  } yield new BacklogCreatorImpl[F](startTimeFinder, recordsFinder, migrationsDSClient)

  def asToBeMigratedInserts(implicit ru: RenkuUrl): List[projects.Path] => Option[SparqlQuery] =
    toTriples andThen toInsertQuery

  private def toTriples(implicit ru: RenkuUrl): List[projects.Path] => List[Triple] =
    _.map(path => Triple(MigrationToV10.name.asEntityId, renku / "toBeMigrated", path.asObject))

  private lazy val toInsertQuery: List[Triple] => Option[SparqlQuery] = {
    case Nil => None
    case triples =>
      val inserts = triples.map(_.asSparql.sparql).mkString("\n\t", "\n\t", "\n")
      SparqlQuery
        .ofUnsafe(
          show"${MigrationToV10.name} - store to backlog",
          s"INSERT DATA {$inserts}"
        )
        .some
  }
}

private class BacklogCreatorImpl[F[_]: Async](startTimeFinder: MigrationStartTimeFinder[F],
                                              recordsFinder:      RecordsFinder[F],
                                              migrationsDSClient: TSClient[F]
)(implicit
    ru: RenkuUrl
) extends BacklogCreator[F] {

  import io.renku.tinytypes.json.TinyTypeDecoders._
  import io.renku.triplesstore.ResultsDecoder._
  import recordsFinder._
  import startTimeFinder._
  import BacklogCreator._

  private val pageSize: Int = 50

  override def createBacklog(): F[Unit] = addPageToBacklog(Ref.unsafe(1))

  private def addPageToBacklog(currentPage: Ref[F, Int]): F[Unit] =
    (findMigrationStartDate -> currentPage.getAndUpdate(_ + 1))
      .mapN(query)
      .flatMap(findRecords[projects.Path])
      .map(asToBeMigratedInserts)
      .flatMap(storeInBacklog(currentPage))

  private def query(startDate: Instant, page: Int) = SparqlQuery.ofUnsafe(
    show"${MigrationToV10.name} - projects to migrate",
    Prefixes of (schema -> "schema", renku -> "renku", xsd -> "xsd"),
    s"""|SELECT DISTINCT ?path
        |WHERE {
        |  {
        |    GRAPH ?id {
        |      ?id a schema:Project;
        |          schema:schemaVersion '9';
        |          renku:projectPath ?path.
        |    }
        |  } UNION {
        |    GRAPH ?id {
        |      ?id a schema:Project;
        |          renku:projectPath ?path;
        |          schema:dateCreated ?created.
        |      OPTIONAL { ?id schema:schemaVersion ?version }
        |      FILTER (!BOUND(?version) && ?created <= ${startDate.asTripleObject.asSparql.sparql})
        |    }
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

  private def storeInBacklog(currentPage: Ref[F, Int]): Option[SparqlQuery] => F[Unit] = {
    case None        => ().pure[F]
    case Some(query) => migrationsDSClient.updateWithNoResult(query) >> addPageToBacklog(currentPage)
  }
}
