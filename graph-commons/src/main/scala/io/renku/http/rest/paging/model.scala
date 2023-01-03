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

package io.renku.http.rest.paging

import io.renku.tinytypes.constraints.{NonNegativeInt, PositiveInt}
import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}

object model {

  final class Page private (val value: Int) extends AnyVal with IntTinyType
  implicit object Page extends TinyTypeFactory[Page](new Page(_)) with PositiveInt[Page] {
    val first: Page = Page(1)
  }

  final class PerPage private (val value: Int) extends AnyVal with IntTinyType
  implicit object PerPage extends TinyTypeFactory[PerPage](new PerPage(_)) with PositiveInt[PerPage] {
    addConstraint(_ <= max.value, v => s"'$v' not a valid $typeName value. Max value is ${PerPage.max}")

    lazy val default: PerPage = PerPage(20)
    lazy val max:     PerPage = new PerPage(100)
  }

  final class Total private (val value: Int) extends AnyVal with IntTinyType
  implicit object Total                      extends TinyTypeFactory[Total](new Total(_)) with NonNegativeInt[Total]
}
