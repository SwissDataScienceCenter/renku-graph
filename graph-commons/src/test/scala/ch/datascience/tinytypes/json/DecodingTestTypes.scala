/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.tinytypes.json

import java.time.{Instant, LocalDate}

import ch.datascience.tinytypes._

private object DecodingTestTypes {

  class StringTestType private (val value: String) extends AnyVal with StringTinyType
  implicit object StringTestType extends TinyTypeFactory[StringTestType](new StringTestType(_)) {
    val InvalidValue: String = "invalid value"
    addConstraint(
      check   = _ != InvalidValue,
      message = (_: String) => s"$typeName cannot be '$InvalidValue'"
    )
  }

  class RelativePathTestType private (val value: String) extends AnyVal with RelativePathTinyType
  implicit object RelativePathTestType extends TinyTypeFactory[RelativePathTestType](new RelativePathTestType(_))

  class UrlTestType private (val value: String) extends AnyVal with UrlTinyType
  implicit object UrlTestType extends TinyTypeFactory[UrlTestType](new UrlTestType(_))

  class IntTestType private (val value: Int) extends AnyVal with IntTinyType
  implicit object IntTestType extends TinyTypeFactory[IntTestType](new IntTestType(_))

  class LongTestType private (val value: Long) extends AnyVal with LongTinyType
  implicit object LongTestType extends TinyTypeFactory[LongTestType](new LongTestType(_))

  class LocalDateTestType private (val value: LocalDate) extends AnyVal with LocalDateTinyType
  implicit object LocalDateTestType extends TinyTypeFactory[LocalDateTestType](new LocalDateTestType(_))

  class InstantTestType private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object InstantTestType extends TinyTypeFactory[InstantTestType](new InstantTestType(_))
}
