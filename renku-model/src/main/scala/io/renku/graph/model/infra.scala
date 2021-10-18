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

package io.renku.graph.model

import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.graph.model.views.{TinyTypeJsonLDOps, UrlResourceRenderer}
import io.renku.tinytypes.constraints.{NonBlank, Url, UrlOps}
import io.renku.tinytypes.json.TinyTypeDecoders
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}

final class RenkuBaseUrl private (val value: String) extends AnyVal with UrlTinyType
object RenkuBaseUrl
    extends TinyTypeFactory[RenkuBaseUrl](new RenkuBaseUrl(_))
    with Url
    with UrlOps[RenkuBaseUrl]
    with UrlResourceRenderer[RenkuBaseUrl]

final class GitLabUrl private (val value: String) extends AnyVal with UrlTinyType {
  def apiV4: GitLabApiUrl = GitLabApiUrl(this)
}
object GitLabUrl extends TinyTypeFactory[GitLabUrl](new GitLabUrl(_)) with Url with UrlOps[GitLabUrl]

final class GitLabApiUrl private (val value: String) extends AnyVal with UrlTinyType
object GitLabApiUrl
    extends TinyTypeFactory[GitLabApiUrl](new GitLabApiUrl(_))
    with Url
    with UrlOps[GitLabApiUrl]
    with UrlResourceRenderer[GitLabApiUrl] {
  def apply(gitLabUrl: GitLabUrl): GitLabApiUrl = new GitLabApiUrl((gitLabUrl / "api" / "v4").value)
}

final class CliVersion private (val value: String) extends AnyVal with StringTinyType
object CliVersion
    extends TinyTypeFactory[CliVersion](new CliVersion(_))
    with NonBlank
    with TinyTypeJsonLDOps[CliVersion] {
  implicit val jsonDecoder: Decoder[CliVersion] = TinyTypeDecoders.stringDecoder(this)
}

final case class RenkuVersionPair(cliVersion: CliVersion, schemaVersion: SchemaVersion)
    extends Product
    with Serializable

object RenkuVersionPair {
  implicit lazy val versionPairDecoder: Decoder[List[RenkuVersionPair]] = { topCursor =>
    val renkuVersionPairs: Decoder[RenkuVersionPair] = { cursor =>
      for {
        cliVersion    <- cursor.downField("cliVersion").downField("value").as[CliVersion]
        schemaVersion <- cursor.downField("schemaVersion").downField("value").as[SchemaVersion]
      } yield RenkuVersionPair(cliVersion, schemaVersion)
    }
    topCursor.downField("results").downField("bindings").as(decodeList(renkuVersionPairs))
  }
}

final class SchemaVersion private (val value: String) extends AnyVal with StringTinyType
object SchemaVersion
    extends TinyTypeFactory[SchemaVersion](new SchemaVersion(_))
    with NonBlank
    with TinyTypeJsonLDOps[SchemaVersion] {
  implicit val jsonDecoder: Decoder[SchemaVersion] = TinyTypeDecoders.stringDecoder(SchemaVersion)
}
