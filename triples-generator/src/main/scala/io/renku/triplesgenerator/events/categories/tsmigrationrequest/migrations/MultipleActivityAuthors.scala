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
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{activities, persons}
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.ResultsDecoder._
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.typelevel.log4cats.Logger
import tooling._

private class MultipleActivityAuthors[F[_]: MonadThrow: Logger](
    executionRegister: MigrationExecutionRegister[F],
    recordsFinder:     RecordsFinder[F],
    updatesRunner:     UpdateQueryRunner[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](Migration.Name("Multiple Activity Authors"), executionRegister, recoveryStrategy) {

  import recordsFinder._
  import recoveryStrategy._

  protected override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (findRawRows.map(d => d.groupBy(_._1).collect(oneAuthor).flatten.toList) >>= (_.map(updateTS).sequence.void))
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private type RawRecord = (activities.ResourceId, persons.ResourceId, Option[String])

  private def findRawRows: F[List[RawRecord]] = {
    implicit val decoder: Decoder[List[RawRecord]] = ResultsDecoder[List, RawRecord] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      for {
        activityId    <- extract[activities.ResourceId]("actId")
        authorId      <- extract[persons.ResourceId]("personId")
        maybeSameAsId <- extract[Option[String]]("maybeSameAs")
      } yield (activityId, authorId, maybeSameAsId)
    }

    findRecords[RawRecord](
      SparqlQuery.of(
        "TS migration: find activity authors",
        Prefixes of (prov -> "prov", schema -> "schema"),
        s"""|SELECT ?actId ?personId ?maybeSameAs
            |WHERE {
            |  {
            |    SELECT ?actId
            |    WHERE {
            |      ?actId a prov:Activity;
            |            prov:wasAssociatedWith ?personId.
            |      ?personId a schema:Person.
            |    }
            |    GROUP BY ?actId
            |    HAVING (COUNT(?personId) > 1)
            |    ORDER BY ?actId
            |  }
            |  ?actId prov:wasAssociatedWith ?personId.
            |  ?personId a schema:Person.
            |  OPTIONAL { ?personId schema:sameAs ?maybeSameAs }
            |}
            |ORDER BY ?actId
            |""".stripMargin
      )
    )
  }

  private type Record = (activities.ResourceId, persons.ResourceId)
  private lazy val oneAuthor: PartialFunction[(activities.ResourceId, List[RawRecord]), Option[Record]] = {
    case (_, rawRecords) =>
      rawRecords
        .find { case (_, _, maybeSameAs) => maybeSameAs.isDefined }
        .orElse(rawRecords.headOption)
        .map { case (activityId, personId, _) => activityId -> personId }
  }

  private lazy val updateTS: Record => F[Unit] = { case (activityId, personId) =>
    updatesRunner.run {
      SparqlQuery.of(
        "TS migration: remove spare activity authors",
        Prefixes of (prov -> "prov", schema -> "schema"),
        s"""|DELETE { ?activityId prov:wasAssociatedWith ?personId }
            |WHERE {
            |  BIND (${activityId.showAs[RdfResource]} AS ?activityId)
            |  ?activityId a prov:Activity;
            |              prov:wasAssociatedWith ?personId.
            |  ?personId a schema:Person.
            |  FILTER (?personId != ${personId.showAs[RdfResource]})
            |}
            |""".stripMargin
      )
    }
  }
}

private object MultipleActivityAuthors {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[MultipleActivityAuthors[F]] = for {
    executionRegister <- MigrationExecutionRegister[F]
    recordsFinder     <- RecordsFinder[F]
    updatesRunner     <- UpdateQueryRunner[F]
  } yield new MultipleActivityAuthors[F](executionRegister, recordsFinder, updatesRunner)
}
