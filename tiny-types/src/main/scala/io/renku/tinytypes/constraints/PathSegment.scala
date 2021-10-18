/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes.constraints

import UrlEncoder.urlEncode
import cats.syntax.all._
import io.renku.tinytypes.{RelativePathTinyType, TinyTypeFactory}

final class PathSegment private (val value: String) extends AnyVal with RelativePathTinyType
object PathSegment extends TinyTypeFactory[PathSegment](new PathSegment(_)) with RelativePath {
  override val transform: String => Either[Throwable, String] =
    value => Either.catchNonFatal(urlEncode(value))
}
