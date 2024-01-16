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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations.v10migration

import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas._
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

private trait MigrationNeedChecker[F[_]] {
  def checkMigrationNeeded: F[ConditionedMigration.MigrationRequired]
}

private object MigrationNeedChecker {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[MigrationNeedChecker[F]] =
    ProjectsConnectionConfig.fromConfig[F]().map(TSClient[F](_)).map(new MigrationNeedCheckerImpl[F](_))
}

private class MigrationNeedCheckerImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends MigrationNeedChecker[F] {

  override def checkMigrationNeeded: F[ConditionedMigration.MigrationRequired] =
    tsClient.queryExpecting[Int](query).map {
      case 0       => ConditionedMigration.MigrationRequired.No("all v9 projects migrated")
      case nonZero => ConditionedMigration.MigrationRequired.Yes(s"$nonZero v9 projects for migration")
    }

  private lazy val query = SparqlQuery.ofUnsafe(
    show"${MigrationToV10.name} - check migration needed",
    Prefixes of (schema -> "schema", renku -> "renku", xsd -> "xsd"),
    s"""|SELECT (COUNT(DISTINCT ?slug) AS ?cnt)
        |WHERE {
        |  GRAPH ?id {
        |    ?id a schema:Project;
        |        schema:schemaVersion '9';
        |        renku:projectPath ?slug.
        |  }
        |}
        |LIMIT 1
        |""".stripMargin
  )

  import io.renku.triplesstore.ResultsDecoder._

  private implicit lazy val decoder: Decoder[Int] = ResultsDecoder.single[Int] { implicit cur =>
    extract[String]("cnt").map(_.toInt)
  }
}
