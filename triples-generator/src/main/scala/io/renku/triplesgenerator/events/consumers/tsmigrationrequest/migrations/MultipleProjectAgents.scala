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
import io.renku.graph.model.CliVersion
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects.ResourceId
import io.renku.graph.model.views.RdfResource
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import org.typelevel.log4cats.Logger
import tooling._

private class MultipleProjectAgents[F[_]: MonadThrow: Logger](
    executionRegister: MigrationExecutionRegister[F],
    recordsFinder:     RecordsFinder[F],
    updatesRunner:     UpdateQueryRunner[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](Migration.Name("Multiple Project agents"), executionRegister, recoveryStrategy) {

  import recordsFinder._
  import recoveryStrategy._

  protected override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (findRawRows >>= (_.map(pickLastVersion).sequence.map(_.flatten)) >>= (_.map(updateTS).sequence.void))
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private type RawRecord = (ResourceId, String)

  private val separator = ","
  private def findRawRows: F[List[RawRecord]] = {
    implicit val decoder: Decoder[List[RawRecord]] = ResultsDecoder[List, RawRecord] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (extract[ResourceId]("id") -> extract[String]("agents")).mapN(_ -> _)
    }

    findRecords[RawRecord](
      SparqlQuery.of(
        "TS migration: find project agents",
        Prefixes of (schema -> "schema"),
        s"""|SELECT ?id (GROUP_CONCAT(DISTINCT ?agent; separator='$separator') AS ?agents)
            |WHERE {
            |  SELECT DISTINCT ?id ?agent
            |  WHERE {
            |    {
            |      SELECT DISTINCT ?id
            |      WHERE {
            |        ?id a schema:Project;
            |              schema:agent ?agent.
            |      }
            |      GROUP BY ?id
            |      HAVING (COUNT(?agent) > 1)
            |    }
            |    ?id a schema:Project;
            |          schema:agent ?agent.
            |  }
            |}
            |GROUP BY ?id
            |""".stripMargin
      )
    )
  }

  private type Record = (ResourceId, CliVersion)
  private lazy val pickLastVersion: RawRecord => F[Option[Record]] = { case (id, versions) =>
    versions
      .split(separator)
      .toList
      .map(CliVersion.from)
      .sequence
      .fold(_.raiseError[F, Option[Record]], _.sorted.lastOption.map(id -> _).pure[F])
  }

  private lazy val updateTS: Record => F[Unit] = { case (id, agent) =>
    updatesRunner.run {
      SparqlQuery.of(
        "TS migration: remove spare project agents",
        Prefixes of (schema -> "schema"),
        s"""|DELETE { ?id schema:agent ?agent }
            |WHERE {
            |  BIND (${id.showAs[RdfResource]} AS ?id)
            |  ?id a schema:Project;
            |      schema:agent ?agent.
            |  FILTER (?agent != '${agent.show}')
            |}
            |""".stripMargin
      )
    }
  }
}

private object MultipleProjectAgents {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[MultipleProjectAgents[F]] = for {
    executionRegister <- MigrationExecutionRegister[F]
    recordsFinder     <- RecordsFinder[F]
    updatesRunner     <- UpdateQueryRunner[F]
  } yield new MultipleProjectAgents[F](executionRegister, recordsFinder, updatesRunner)
}
