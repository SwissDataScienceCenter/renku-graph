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

package io.renku.graph.model

import cats.syntax.all._
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.graph.model.views.{EntityIdJsonLdOps, RdfResource}
import io.renku.tinytypes.constraints.Url
import io.renku.tinytypes.{Renderer, StringTinyType, TinyTypeFactory}

object associations {
  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with EntityIdJsonLdOps[ResourceId] {
    implicit object RdfResourceRenderer extends Renderer[RdfResource, ResourceId] {
      override def render(id: ResourceId): String = s"<${sparqlEncode(id.show)}>"
    }
  }
}
