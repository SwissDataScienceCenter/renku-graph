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

package io.renku.graph.model

import cats.syntax.all._
import cats.Show
import io.renku.graph.model.entityModel.Location.FileOrFolder.from
import io.renku.graph.model.views.{EntityIdJsonLDOps, TinyTypeJsonLDOps}
import io.renku.jsonld.JsonLDDecoder.decodeString
import io.renku.jsonld.JsonLDEncoder.encodeString
import io.renku.jsonld.{JsonLDDecoder, JsonLDEncoder}
import io.renku.tinytypes.constraints.{NonBlank, RelativePath, Url}
import io.renku.tinytypes.{RelativePathTinyType, StringTinyType, TinyTypeFactory}

object entityModel {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url[ResourceId]
      with EntityIdJsonLDOps[ResourceId]

  sealed trait LocationLike extends Any with RelativePathTinyType {
    override def equals(obj: Any): Boolean =
      Option(obj).exists {
        case v: LocationLike => v.value == value
        case _ => false
      }

    override def hashCode(): Int = value.hashCode
  }

  object LocationLike {
    implicit lazy val jsonLDDecoder: JsonLDDecoder[LocationLike] =
      decodeString.emap(value => from(value).leftMap(_.getMessage))

    implicit lazy val jsonLDEncoder: JsonLDEncoder[LocationLike] = encodeString.contramap(_.value)
  }

  sealed trait Location extends Any with LocationLike {
    def fold[A](ifFolder: Location.Folder => A, ifFile: Location.File => A): A
  }

  object Location {

    final class File private (val value: String) extends Location {
      def fold[A](ifFolder: Location.Folder => A, ifFile: Location.File => A): A = ifFile(this)
    }
    object File extends TinyTypeFactory[File](new File(_)) with RelativePath[File] with TinyTypeJsonLDOps[File] {
      def apply(folder: Location.Folder, filename: String): Location.File = Location.File(s"$folder/$filename")
    }

    final class Folder private (val value: String) extends Location {
      def fold[A](ifFolder: Location.Folder => A, ifFile: Location.File => A): A = ifFolder(this)
    }
    object Folder
        extends TinyTypeFactory[Folder](new Folder(_))
        with RelativePath[Folder]
        with TinyTypeJsonLDOps[Folder]

    final class FileOrFolder private (val value: String) extends LocationLike
    object FileOrFolder extends TinyTypeFactory[FileOrFolder](new FileOrFolder(_)) with RelativePath[FileOrFolder] {
      implicit lazy val jsonLDDecoder: JsonLDDecoder[FileOrFolder] =
        decodeString.emap(value => from(value).leftMap(_.getMessage))
    }

    implicit lazy val jsonLDEncoder: JsonLDEncoder[Location] = encodeString.contramap(_.value)

    implicit val show: Show[Location] = Show.show {
      case Location.File(v)   => v
      case Location.Folder(v) => v
    }
  }

  final class Checksum private (val value: String) extends AnyVal with StringTinyType
  implicit object Checksum
      extends TinyTypeFactory[Checksum](new Checksum(_))
      with NonBlank[Checksum]
      with TinyTypeJsonLDOps[Checksum]
}
