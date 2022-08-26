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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.projects.{Path, ResourceId}
import io.renku.graph.model.views.RdfResource
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger
import tooling._

private class AddProjectNamespaceField[F[_]: MonadThrow: Logger](
    executionRegister: MigrationExecutionRegister[F],
    recordsFinder:     RecordsFinder[F],
    updatesRunner:     UpdateQueryRunner[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](Migration.Name("Add renku:projectNamespace properties"),
                                 executionRegister,
                                 recoveryStrategy
    ) {

  import recordsFinder._
  import recoveryStrategy._

  protected override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (findRows >>= (_.map(updateTS).sequence.void))
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private type Record = (ResourceId, Path)

  private def findRows: F[List[Record]] = {
    implicit val decoder: Decoder[List[Record]] = ResultsDecoder[List, Record] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (extract[ResourceId]("id") -> extract[Path]("path")).mapN(_ -> _)
    }

    findRecords[Record](
      SparqlQuery.of(
        "TS migration: find projects paths",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""|SELECT ?id ?path
            |WHERE {
            |  ?id a schema:Project;
            |      renku:projectPath ?path.
            |  FILTER NOT EXISTS {
            |    ?id renku:projectNamespace ?namespace
            |  }
            |}
            |""".stripMargin
      )
    )
  }

  private lazy val updateTS: Record => F[Unit] = { case (id, path) =>
    updatesRunner.run {
      SparqlQuery.of(
        "TS migration: add missing renku:projectNamespace",
        Prefixes of renku -> "renku",
        s"""INSERT DATA { ${id.showAs[RdfResource]} renku:projectNamespace '${path.toNamespace}' }"""
      )
    }
  }
}

private object AddProjectNamespaceField {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[AddProjectNamespaceField[F]] = for {
    executionRegister <- MigrationExecutionRegister[F]
    recordsFinder     <- RecordsFinder[F]
    updatesRunner     <- UpdateQueryRunner[F]
  } yield new AddProjectNamespaceField[F](executionRegister, recordsFinder, updatesRunner)
}
