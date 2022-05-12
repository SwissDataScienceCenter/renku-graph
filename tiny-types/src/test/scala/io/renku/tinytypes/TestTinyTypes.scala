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

package io.renku.tinytypes

import io.renku.tinytypes.constraints.{FiniteFloat, Url, UrlOps}
import io.renku.tinytypes.contenttypes.ZippedContent

import java.time.{Duration, Instant, LocalDate}

object TestTinyTypes {

  class StringTestType private (val value: String) extends AnyVal with StringTinyType
  implicit object StringTestType extends TinyTypeFactory[StringTestType](new StringTestType(_)) {
    val InvalidValue: String = "invalid value"
    addConstraint(
      check = _ != InvalidValue,
      message = (_: String) => s"$typeName cannot be '$InvalidValue'"
    )
  }

  class RelativePathTestType private (val value: String) extends AnyVal with RelativePathTinyType
  implicit object RelativePathTestType extends TinyTypeFactory[RelativePathTestType](new RelativePathTestType(_))

  class UrlTestType private (val value: String) extends AnyVal with UrlTinyType
  implicit object UrlTestType
      extends TinyTypeFactory[UrlTestType](new UrlTestType(_))
      with Url[UrlTestType]
      with UrlOps[UrlTestType]

  class IntTestType private (val value: Int) extends AnyVal with IntTinyType
  implicit object IntTestType                extends TinyTypeFactory[IntTestType](new IntTestType(_))

  class LongTestType private (val value: Long) extends AnyVal with LongTinyType
  implicit object LongTestType                 extends TinyTypeFactory[LongTestType](new LongTestType(_))

  class FloatTestType private (val value: Float) extends AnyVal with FloatTinyType
  implicit object FloatTestType
      extends TinyTypeFactory[FloatTestType](new FloatTestType(_))
      with FiniteFloat[FloatTestType]

  class LocalDateTestType private (val value: LocalDate) extends AnyVal with LocalDateTinyType

  implicit object LocalDateTestType extends TinyTypeFactory[LocalDateTestType](new LocalDateTestType(_))

  class InstantTestType private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object InstantTestType                    extends TinyTypeFactory[InstantTestType](new InstantTestType(_))

  class DurationTestType private (val value: Duration) extends AnyVal with DurationTinyType

  implicit object DurationTestType extends TinyTypeFactory[DurationTestType](new DurationTestType(_))

  class ByteArrayTestType private (val value: Array[Byte]) extends AnyVal with ByteArrayTinyType with ZippedContent
  implicit object ByteArrayTestType extends TinyTypeFactory[ByteArrayTestType](new ByteArrayTestType(_))
}
