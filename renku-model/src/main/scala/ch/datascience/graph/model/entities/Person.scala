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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.graph.model.GitLabApiUrl
import ch.datascience.graph.model.users.{Affiliation, Email, GitLabId, Name, ResourceId}
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDEncoder.encodeOption
import io.renku.jsonld._

final case class Person(
    resourceId:       ResourceId,
    name:             Name,
    maybeEmail:       Option[Email] = None,
    maybeAffiliation: Option[Affiliation] = None,
    maybeGitLabId:    Option[GitLabId] = None
)

object Person {

  import ch.datascience.graph.model.Schemas._
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.syntax._

  private val entityTypes = EntityTypes.of(prov / "Person", schema / "Person")

  implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Person] =
    JsonLDEncoder.instance { person =>
      JsonLD.entity(
        person.resourceId.asEntityId,
        entityTypes,
        schema / "email"       -> person.maybeEmail.asJsonLD,
        schema / "name"        -> person.name.asJsonLD,
        rdfs / "label"         -> person.name.asJsonLD,
        schema / "affiliation" -> person.maybeAffiliation.asJsonLD,
        schema / "sameAs"      -> person.maybeGitLabId.asJsonLD(encodeOption(gitLabIdEncoder))
      )
    }

  private val gitLabSameAsTypes:          EntityTypes = EntityTypes.of(schema / "URL")
  private val gitLabSameAsAdditionalType: String      = "GitLab"

  private def gitLabIdEncoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[GitLabId] = JsonLDEncoder.instance {
    gitLabId =>
      JsonLD.entity(
        EntityId of (gitLabApiUrl / "users" / gitLabId).toString,
        gitLabSameAsTypes,
        schema / "identifier"     -> gitLabId.value.asJsonLD,
        schema / "additionalType" -> gitLabSameAsAdditionalType.asJsonLD
      )
  }

  import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._
  import io.renku.jsonld.JsonLDDecoder._

  implicit lazy val decoder: JsonLDDecoder[Person] = JsonLDDecoder.entity(entityTypes) { cursor =>
    for {
      resourceId       <- cursor.downEntityId.as[ResourceId]
      name             <- cursor.downField(schema / "name").as[Name]
      maybeEmail       <- cursor.downField(schema / "email").as[Option[Email]]
      maybeAffiliation <- cursor.downField(schema / "affiliation").as[Option[Affiliation]]
      maybeGitLabId    <- cursor.downField(schema / "sameAs").as[Option[GitLabId]](decodeOption(gitLabIdDecoder))
    } yield Person(resourceId, name, maybeEmail, maybeAffiliation, maybeGitLabId)
  }

  private lazy val gitLabIdDecoder: JsonLDDecoder[GitLabId] = JsonLDDecoder.entity(gitLabSameAsTypes) { cursor =>
    for {
      _ <- cursor.downField(schema / "additionalType").as[String].flatMap {
             case `gitLabSameAsAdditionalType` => ().asRight
             case additionalType               => DecodingFailure(s"Unknown $additionalType type of Person SameAs", Nil).asLeft
           }
      gitLabId <- cursor.downField(schema / "identifier").as[GitLabId]
    } yield gitLabId
  }
}
