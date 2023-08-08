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

package io.renku.triplesgenerator.projects.update

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.DecodingFailure.Reason
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.projects
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait ProjectExistenceChecker[F[_]] {
  def checkExists(slug: projects.Slug): F[Boolean]
}

private object ProjectExistenceChecker {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: DatasetConnectionConfig
  ): ProjectExistenceChecker[F] =
    new ProjectExistenceCheckerImpl[F](TSClient[F](connectionConfig))
}

private class ProjectExistenceCheckerImpl[F[_]](tsClient: TSClient[F]) extends ProjectExistenceChecker[F] {

  def checkExists(slug: projects.Slug): F[Boolean] =
    tsClient.queryExpecting(query(slug))(decoder(slug))

  private def query(slug: projects.Slug) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: check project exists",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT (COUNT(DISTINCT ?id) AS ?count)
               |WHERE {
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:slug ${slug.asObject}
               |  }
               |}
               |""".stripMargin
    )

  private def decoder(slug: projects.Slug): Decoder[Boolean] =
    ResultsDecoder.single[Boolean](implicit cur =>
      extract[String]("count").map(_.toInt) >>= {
        case 0 => false.asRight[DecodingFailure]
        case 1 => true.asRight[DecodingFailure]
        case _ => DecodingFailure(Reason.CustomReason(show"Cannot determine $slug existence"), cur).asLeft[Boolean]
      }
    )
}
