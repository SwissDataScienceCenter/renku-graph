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

package io.renku.core.client

import io.circe.Decoder
import io.renku.tinytypes.constraints.{NonBlank, Url}
import io.renku.tinytypes.json.TinyTypeDecoders
import io.renku.tinytypes.{BooleanTinyType, StringTinyType, TinyTypeFactory, UrlTinyType}

final class ApiVersion private (val value: String) extends AnyVal with StringTinyType
object ApiVersion extends TinyTypeFactory[ApiVersion](new ApiVersion(_)) with NonBlank[ApiVersion] {
  implicit val decoder: Decoder[ApiVersion] = TinyTypeDecoders.stringDecoder(ApiVersion)
}

final class MigrationRequired private (val value: Boolean) extends AnyVal with BooleanTinyType
object MigrationRequired extends TinyTypeFactory[MigrationRequired](new MigrationRequired(_)) {
  lazy val yes:         MigrationRequired          = MigrationRequired(true)
  lazy val no:          MigrationRequired          = MigrationRequired(false)
  implicit val decoder: Decoder[MigrationRequired] = TinyTypeDecoders.booleanDecoder(MigrationRequired)
}

final class ProjectRepository private (val value: String) extends AnyVal with UrlTinyType
object ProjectRepository
    extends TinyTypeFactory[ProjectRepository](new ProjectRepository(_))
    with Url[ProjectRepository]

final class Branch private (val value: String) extends AnyVal with StringTinyType
object Branch extends TinyTypeFactory[Branch](new Branch(_)) with NonBlank[Branch] {

  implicit val factory: TinyTypeFactory[Branch] = this

  val default: Branch = Branch("main")

  implicit val decoder: Decoder[Branch] = TinyTypeDecoders.stringDecoder(Branch)
}
