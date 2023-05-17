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

package io.renku.knowledgegraph.ontology

import cats.effect.Async
import cats.syntax.all._
import fs2.io.file.Files
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Accept, Location}
import org.http4s.{Headers, Request, Response, StaticFile, Uri}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `GET /ontology`(path: Uri.Path)(implicit request: Request[F]): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: Logger]: F[Endpoint[F]] =
    new EndpointImpl[F](OntologyGenerator(), HtmlGenerator[F]).pure[F].widen
}

private class EndpointImpl[F[_]: Async: Logger](ontologyGenerator: OntologyGenerator, htmlGenerator: HtmlGenerator[F])
    extends Http4sDsl[F]
    with Endpoint[F] {

  import fs2.io.file.Path.fromNioPath
  import io.renku.http.jsonld4s._
  import io.renku.http.server.endpoint._
  import org.http4s.MediaType.{application, text}

  override def `GET /ontology`(path: Uri.Path)(implicit request: Request[F]): F[Response[F]] =
    whenAccept(
      text.html              --> pageResponse(path),
      text.plain             --> pageResponse(path),
      text.css               --> pageResponse(path),
      application.javascript --> pageResponse(path),
      application.json       --> pageResponse(path),
      application.`ld+json`  --> Ok(ontologyGenerator.getOntology)
    )(default = pageResponse(path))

  private def pageResponse(path: Uri.Path)(implicit request: Request[F]) = htmlGenerator.generateHtml >> {
    if (path.isEmpty)
      Response[F](SeeOther, headers = Headers(Location(request.uri / "index-en.html"), Accept(text.html))).pure[F]
    else {
      implicit val files: Files[F] = Files.forAsync
      StaticFile
        .fromPath(fromNioPath(htmlGenerator.generationPath resolve path.toString()), Some(request))
        .getOrElseF(NotFound(s"Ontology '$path' resource cannot be found"))
    }
  }
}
