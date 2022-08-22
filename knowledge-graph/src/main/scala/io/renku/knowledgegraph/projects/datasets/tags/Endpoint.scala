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

package io.renku.knowledgegraph.projects.datasets.tags

import Endpoint._
import cats.effect.Async
import cats.syntax.all._
import io.renku.config.renku
import io.renku.graph.model
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `GET /projects/:path/datasets/:name/tags`(criteria: Criteria)(implicit request: Request[F]): F[Response[F]]
}

object Endpoint {

  final case class Criteria(projectPath: model.projects.Path,
                            datasetName: model.datasets.Name,
                            maybeUser:   Option[AuthUser] = None
  )

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    renkuApiUrl <- renku.ApiUrl()
  } yield new EndpointImpl(renkuApiUrl)
}

private class EndpointImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](renkuApiUrl: renku.ApiUrl)
    extends Endpoint[F] {
  println(renkuApiUrl)
  override def `GET /projects/:path/datasets/:name/tags`(criteria: Criteria)(implicit request: Request[F]) = ???
}
