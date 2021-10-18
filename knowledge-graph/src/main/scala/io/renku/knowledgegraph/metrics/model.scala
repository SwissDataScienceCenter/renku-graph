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

package io.renku.knowledgegraph.metrics

import io.circe.Decoder
import io.renku.graph.model.Schemas
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.json.TinyTypeDecoders.longDecoder
import io.renku.tinytypes.{LongTinyType, StringTinyType, TinyTypeFactory}

private final class Count private (val value: Long) extends AnyVal with LongTinyType
private object Count extends TinyTypeFactory[Count](new Count(_)) {
  implicit val decoder: Decoder[Count] = longDecoder(Count)
  addConstraint(
    check = _ >= 0L,
    message = _ => s"$typeName has to be >= 0"
  )
}

private final class EntityLabel private (val value: String) extends AnyVal with StringTinyType
private object EntityLabel extends TinyTypeFactory[EntityLabel](new EntityLabel(_)) with NonBlank {
  private val allSchemas = Schemas.all.map(_.toString)
  override val transform: String => Either[Throwable, String] = entityType =>
    allSchemas
      .find(schema => entityType.startsWith(schema))
      .map(schema => entityType.replace(schema, ""))
      .toRight(left = new IllegalArgumentException(s"$entityType not recognizable"))
}
