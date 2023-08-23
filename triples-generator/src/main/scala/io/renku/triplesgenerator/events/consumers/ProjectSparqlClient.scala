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

package io.renku.triplesgenerator.events.consumers

import cats.Monad
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.auto._
import fs2.io.net.Network
import io.renku.jsonld.JsonLD
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import io.renku.triplesstore.client.http.{Retry, SparqlClient, SparqlQuery, SparqlUpdate}
import org.typelevel.log4cats.Logger

/** SparQL client fixed to the `projects` dataset. */
trait ProjectSparqlClient[F[_]] extends SparqlClient[F]

object ProjectSparqlClient {
  def apply[F[_]: Monad: Logger: SparqlQueryTimeRecorder](c: SparqlClient[F]) =
    new ProjectSparqlClient[F] {
      private[this] val rec = SparqlQueryTimeRecorder[F].instance
      override def update(request: SparqlUpdate) = {
        val label = histogramLabel(request)
        val work  = c.update(request)
        rec
          .measureExecutionTime(work, label)
          .flatMap(rec.logExecutionTime(s"Execute sparql update '$label'"))
      }

      override def upload(data: JsonLD) = {
        val label: String Refined NonEmpty = "jsonld upload"
        val work = c.upload(data)
        rec
          .measureExecutionTime(work, label.some)
          .flatMap(rec.logExecutionTime("Execute JSONLD upload"))
      }

      override def query(request: SparqlQuery) = {
        val label = histogramLabel(request)
        val work  = c.query(request)
        rec
          .measureExecutionTime(work, label)
          .flatMap(rec.logExecutionTime(s"Execute sparql query '$label'"))
      }
    }

  def apply[F[_]: Network: Async: Logger: SparqlQueryTimeRecorder](
      cc:       ProjectsConnectionConfig,
      retryCfg: Retry.RetryConfig = Retry.RetryConfig.default
  ): Resource[F, ProjectSparqlClient[F]] = {
    val cfg = cc.toCC(Some(retryCfg))
    SparqlClient[F](cfg).map(apply(_))
  }

  private def histogramLabel(r: Any): Option[String Refined NonEmpty] =
    r match {
      case q: io.renku.triplesstore.SparqlQuery => q.name.some
      case _ => None
    }
}
