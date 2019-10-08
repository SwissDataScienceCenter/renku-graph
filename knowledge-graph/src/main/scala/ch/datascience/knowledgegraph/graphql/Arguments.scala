/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.graphql

import ch.datascience.tinytypes.{From, TinyType, TypeName}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import sangria.ast
import sangria.schema.{ScalarType, valueOutput}
import sangria.validation.ValueCoercionViolation

object Arguments {

  implicit class TinyTypeOps[TT <: TinyType { type V = String }](typeFactory: From[TT] with TypeName) {

    type NonBlank = String Refined NonEmpty

    case class TinyTypeCoercionViolation(message: NonBlank) extends ValueCoercionViolation(message.value)

    def toScalarType(
        description:      NonBlank,
        exceptionMessage: NonBlank = Refined.unsafeApply(s"${typeFactory.shortTypeName} has invalid value")
    ): ScalarType[TT] = {

      import cats.implicits._

      ScalarType[TT](
        name         = typeFactory.shortTypeName,
        description  = Some(description.value),
        coerceOutput = valueOutput,
        coerceUserInput = {
          case s: String => typeFactory.from(s) leftMap (_ => TinyTypeCoercionViolation(exceptionMessage))
          case _ => Left(TinyTypeCoercionViolation(exceptionMessage))
        },
        coerceInput = {
          case ast.StringValue(s, _, _, _, _) =>
            typeFactory.from(s) leftMap (_ => TinyTypeCoercionViolation(exceptionMessage))
          case _ => Left(TinyTypeCoercionViolation(exceptionMessage))
        }
      )
    }
  }
}
