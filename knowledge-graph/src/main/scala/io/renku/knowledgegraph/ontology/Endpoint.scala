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

package io.renku.knowledgegraph.ontology

import cats.effect.Async
import cats.syntax.all._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `GET /ontology`(implicit request: Request[F]): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: Logger]: F[Endpoint[F]] = new EndpointImpl[F](OntologyGenerator(), HtmlGenerator).pure[F].widen
}

private class EndpointImpl[F[_]: Async: Logger](ontologyGenerator: OntologyGenerator, htmlGenerator: HtmlGenerator)
    extends Http4sDsl[F]
    with Endpoint[F] {

  import io.renku.http.jsonld4s._
  import io.renku.http.server.endpoint._
  import org.http4s.headers.`Content-Type`
  import org.http4s.MediaType.{application, text}

  override def `GET /ontology`(implicit request: Request[F]): F[Response[F]] =
    whenAccept(
      text.html             --> htmlResponse,
      application.`ld+json` --> Ok(ontologyGenerator.getOntology)
    )(default = htmlResponse)

  private lazy val htmlResponse = Ok(htmlGenerator.getHtml).map(_.withContentType(`Content-Type`(text.html)))
}
