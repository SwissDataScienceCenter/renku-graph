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

package io.renku.knowledgegraph.docs

import cats.MonadThrow
import cats.implicits._
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

trait EndpointDocs {
  def path: Path
}

private object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[EndpointDocs] = new EndpointDocsImpl().pure[F].widen
}

private class EndpointDocsImpl() extends EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "OpenAPI specification",
      "OpenAPI specification of the service's resources",
      Uri / "spec.json",
      Status.Ok -> Response("Specification in JSON", Contents(MediaType.`application/json`)),
      Status.InternalServerError ->
        Response("Error", Contents(MediaType.`application/json`("Reason", ErrorMessage("Message"))))
    )
  )
}
