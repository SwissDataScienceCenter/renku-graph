/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

import ch.datascience.tinytypes.TinyType
import play.api.libs.json._

import scala.util.Try

class TinyTypeReads[V, T <: TinyType[V]]( instantiate: V => T )( implicit jsonToV: JsValue => JsResult[V] ) extends Reads[T] {
  override def reads( json: JsValue ): JsResult[T] =
    jsonToV( json )
      .flatMap { v =>
        Try( instantiate( v ) ).fold(
          exception => JsError( exception.getMessage ),
          tinyType => JsSuccess( tinyType )
        )
      }
}

object TinyTypeReads {

  def apply[V, T <: TinyType[V]]( instantiate: V => T )( implicit jsonToV: JsValue => JsResult[V] ): Reads[T] =
    new TinyTypeReads( instantiate )
}

class TinyTypeWrites[V, T <: TinyType[V]]()( implicit valueToJson: V => JsValue ) extends Writes[T] {
  override def writes( tinyType: T ): JsValue = valueToJson( tinyType.value )
}

object TinyTypeWrites {

  def apply[V, T <: TinyType[V]]( implicit valueToJson: V => JsValue ): Writes[T] =
    new TinyTypeWrites[V, T]()
}

object TinyTypeFormat {

  def apply[V, T <: TinyType[V]]( instantiate: V => T )( implicit jsonToV: JsValue => JsResult[V], valueToJson: V => JsValue ): Format[T] =
    new Format[T] {
      override def writes( o: T ): JsValue = TinyTypeWrites[V, T].writes( o )

      override def reads( json: JsValue ): JsResult[T] = TinyTypeReads( instantiate ).reads( json )
    }
}
