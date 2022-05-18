/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets.{ResourceId, SameAs}
import io.renku.graph.model.views.RdfResource
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.ResultsDecoding._
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.typelevel.log4cats.Logger
import tooling._

private class MultipleDSSameAs[F[_]: MonadThrow: Logger](
    executionRegister: MigrationExecutionRegister[F],
    recordsFinder:     RecordsFinder[F],
    updatesRunner:     UpdateQueryRunner[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](Migration.Name("Multiple DS SameAs"), executionRegister, recoveryStrategy) {

  import recordsFinder._
  import recoveryStrategy._

  protected override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (findRawRows >>= (_.map(pickLast).sequence.map(_.flatten)) >>= (_.map(updateTS).sequence.void))
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private type RawRecord = (ResourceId, String)

  private val separator = ","
  private def findRawRows: F[List[RawRecord]] = {
    implicit val decoder: Decoder[List[RawRecord]] = ListResultsDecoder[RawRecord] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (extract[ResourceId]("dsId") -> extract[String]("allSameAs")).mapN(_ -> _)
    }

    findRecords[RawRecord](
      SparqlQuery.of(
        "TS migration: find sameAs",
        Prefixes of schema -> "schema",
        s"""|SELECT ?dsId (GROUP_CONCAT(?sameAs; separator='$separator') AS ?allSameAs)
            |WHERE {
            |  SELECT DISTINCT ?dsId ?parentId ?sameAs
            |  WHERE {
            |    {
            |      SELECT ?dsId
            |      WHERE {
            |        ?dsId a schema:Dataset;
            |              schema:sameAs ?sameAs;
            |              schema:sameAs/schema:url ?parentId.
            |      }
            |      GROUP BY ?dsId ?parentId
            |      HAVING (COUNT(?sameAs) > 1)
            |      ORDER BY ?dsId
            |    }
            |    ?dsId a schema:Dataset;
            |          schema:sameAs ?sameAs.
            |    ?dsId (schema:sameAs/schema:url)* ?parentId.
            |    ?parentId schema:sameAs ?sameAs
            |  }
            |}
            |GROUP BY ?dsId
            |""".stripMargin
      )
    )
  }

  private type Record = (ResourceId, SameAs)
  private lazy val pickLast: RawRecord => F[Option[Record]] = { case (dsId, allSameAs) =>
    allSameAs
      .split(separator)
      .toList
      .map(SameAs.from)
      .sequence
      .fold(_.raiseError[F, Option[Record]], _.lastOption.map(dsId -> _).pure[F])
  }

  private lazy val updateTS: Record => F[Unit] = { case (dsId, sameAs) =>
    updatesRunner.run {
      SparqlQuery.of(
        "TS migration: remove spare sameAs",
        Prefixes of schema -> "schema",
        s"""|DELETE { ?dsId schema:sameAs ?sameAs }
            |WHERE {
            |  BIND (${dsId.showAs[RdfResource]} AS ?dsId)
            |  ?dsId a schema:Dataset;
            |        schema:sameAs ?sameAs.
            |  FILTER (?sameAs != ${sameAs.showAs[RdfResource]})
            |}
            |""".stripMargin
      )
    }
  }
}

private object MultipleDSSameAs {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[MultipleDSSameAs[F]] = for {
    executionRegister <- MigrationExecutionRegister[F]
    recordsFinder     <- RecordsFinder[F]
    updatesRunner     <- UpdateQueryRunner[F]
  } yield new MultipleDSSameAs[F](executionRegister, recordsFinder, updatesRunner)
}
