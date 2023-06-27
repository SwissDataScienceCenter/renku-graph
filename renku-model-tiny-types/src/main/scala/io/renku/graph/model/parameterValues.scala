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

package io.renku.graph.model

import io.renku.graph.model.views.{EntityIdJsonLDOps, TinyTypeJsonLDOps}
import io.renku.tinytypes.constraints.Url
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

object parameterValues {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url[ResourceId]
      with EntityIdJsonLDOps[ResourceId]

  final class ValueOverride private (val value: String) extends AnyVal with StringTinyType
  implicit object ValueOverride
      extends TinyTypeFactory[ValueOverride](new ValueOverride(_))
      with TinyTypeJsonLDOps[ValueOverride]
}
