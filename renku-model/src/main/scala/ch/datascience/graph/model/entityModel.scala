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

package ch.datascience.graph.model

import ch.datascience.graph.model.views.EntityIdEncoderOps
import ch.datascience.tinytypes.{RelativePathTinyType, StringTinyType, TinyTypeFactory}
import ch.datascience.tinytypes.constraints.{NonBlank, RelativePath, Url}

object entityModel {
  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with EntityIdEncoderOps[ResourceId]

  sealed trait Location extends Any with RelativePathTinyType
  object Location {

    final class File private (val value: String) extends AnyVal with Location
    object File extends TinyTypeFactory[File](new File(_)) with RelativePath {
      def apply(folder: Location.Folder, filename: String): Location.File = Location.File(s"$folder/$filename")
    }

    final class Folder private (val value: String) extends AnyVal with Location
    object Folder extends TinyTypeFactory[Folder](new Folder(_)) with RelativePath
  }

  final class Checksum private (val value: String) extends AnyVal with StringTinyType
  object Checksum extends TinyTypeFactory[Checksum](new Checksum(_)) with NonBlank

}
