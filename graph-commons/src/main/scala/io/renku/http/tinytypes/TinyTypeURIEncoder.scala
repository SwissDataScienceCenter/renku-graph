/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.http.tinytypes

import io.renku.tinytypes.TinyType
import org.http4s.{QueryParamEncoder, Uri}
import org.http4s.Uri.Path.SegmentEncoder

object TinyTypeURIEncoder extends TinyTypeURIEncoder

// It looks it might be convenient to have this functionality available out of the box for all TinyTypes.
// However, the tiny-types module does not depend on http4s so maybe additional module (like tiny-types-http4s)
// could be created at some point.
trait TinyTypeURIEncoder {

  implicit def segmentEncoder[TT <: TinyType](implicit
      valueEncoder: SegmentEncoder[TT#V]
  ): Uri.Path.SegmentEncoder[TT] = valueEncoder.contramap(_.value)

  implicit def queryParamEncoder[TT <: TinyType](implicit
      valueEncoder: QueryParamEncoder[TT#V]
  ): QueryParamEncoder[TT] = valueEncoder.contramap(_.value)
}
