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

package io.renku.graph.model

import cats.syntax.all._
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.graph.model.views.{EntityIdJsonLdOps, RdfResource, TinyTypeJsonLDOps}
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{NonBlank, NonNegativeInt}

object persons {

  sealed trait ResourceId extends Any with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](ResourceIdFactory)
      with Constraints[ResourceId]
      with EntityIdJsonLdOps[ResourceId] {

    addConstraint(
      check = value =>
        value.trim.matches(GitLabIdBased.validator) ||
          value.trim.matches(OrcidId.validator) ||
          value.trim.matches(EmailBased.validator) ||
          value.trim.matches(NameBased.validator),
      message = (v: String) => s"$v is not a valid $typeName"
    )

    final class GitLabIdBased private[persons] (val value: String) extends AnyVal with ResourceId
    object GitLabIdBased
        extends TinyTypeFactory[GitLabIdBased](new GitLabIdBased(_))
        with EntityIdJsonLdOps[GitLabIdBased]
        with Constraints[GitLabIdBased]
        with NonBlank[GitLabIdBased] {
      private[persons] val validator = "^http(s)?://.*/persons/\\d+$"
      addConstraint(
        check = _.trim.matches(validator),
        message = (v: String) => s"$v is not valid $typeName"
      )
    }

    final class OrcidIdBased private[persons] (val value: String) extends AnyVal with ResourceId
    object OrcidIdBased
        extends TinyTypeFactory[OrcidIdBased](new OrcidIdBased(_))
        with EntityIdJsonLdOps[OrcidIdBased]
        with Constraints[OrcidIdBased]
        with NonBlank[OrcidIdBased] {
      private[persons] val validator = "^http(s)?://.*/persons\\/orcid\\/\\d{4}-\\d{4}-\\d{4}-\\d{4}$"
      addConstraint(
        check = _.trim.matches(validator),
        message = (v: String) => s"$v is not valid $typeName"
      )
    }

    final class EmailBased private[persons] (val value: String) extends AnyVal with ResourceId
    object EmailBased
        extends TinyTypeFactory[EmailBased](new EmailBased(_))
        with EntityIdJsonLdOps[EmailBased]
        with Constraints[EmailBased]
        with NonBlank[EmailBased] {
      private[persons] val validator = "^mailto:(.*)@.*$"
      addConstraint(
        check = _.trim.matches(validator),
        message = (v: String) => s"$v is not valid $typeName"
      )
    }

    final class NameBased private[persons] (val value: String) extends AnyVal with ResourceId
    object NameBased
        extends TinyTypeFactory[NameBased](new NameBased(_))
        with EntityIdJsonLdOps[NameBased]
        with Constraints[NameBased]
        with NonBlank[NameBased] {
      private[persons] val validator = "^http(s)?://.*/persons/.+$"
      addConstraint(
        check = _.trim.matches(validator),
        message = (v: String) => s"$v is not valid $typeName"
      )
    }

    def apply(gitLabId: GitLabId)(implicit renkuUrl: RenkuUrl): GitLabIdBased =
      new GitLabIdBased((renkuUrl / "persons" / gitLabId).show)

    def apply(orcidId: OrcidId)(implicit renkuUrl: RenkuUrl, ev: OrcidId.type): OrcidIdBased =
      new OrcidIdBased((renkuUrl / "persons" / "orcid" / orcidId.id).show)

    def apply(email: Email): EmailBased = new EmailBased(show"mailto:$email")

    def apply(name: Name)(implicit renkuUrl: RenkuUrl): NameBased =
      new NameBased((renkuUrl / "persons" / name).show)

    implicit object UsersResourceIdRdfResourceRenderer extends Renderer[RdfResource, ResourceId] {
      private val localPartExtractor = "^mailto:(.*)@.*$".r

      override def render(id: ResourceId): String = id.value match {
        case localPartExtractor(localPart) => s"<${id.value.replace(localPart, sparqlEncode(localPart))}>"
        case otherId                       => s"<${sparqlEncode(otherId)}>"
      }
    }
  }

  private object ResourceIdFactory extends (String => ResourceId) {
    override def apply(value: String): ResourceId =
      ResourceId.GitLabIdBased
        .from(value)
        .orElse(ResourceId.OrcidIdBased.from(value))
        .orElse(ResourceId.EmailBased.from(value))
        .orElse(ResourceId.NameBased.from(value))
        .getOrElse(throw new IllegalArgumentException(s"$value is not a valid ${ResourceId.typeName}"))
  }

  final class GitLabId private (val value: Int) extends AnyVal with IntTinyType
  implicit object GitLabId
      extends TinyTypeFactory[GitLabId](new GitLabId(_))
      with NonNegativeInt[GitLabId]
      with TinyTypeJsonLDOps[GitLabId] {

    def parse(value: String): Either[IllegalArgumentException, GitLabId] =
      Either
        .fromOption(value.toIntOption, ifNone = new IllegalArgumentException(s"$value not a valid GitLabId"))
        .flatMap(GitLabId.from)
  }

  final class OrcidId private[persons] (val value: String) extends AnyVal with StringTinyType {
    import OrcidId.validatorRegex

    def id: String = validatorRegex
      .findAllIn(value)
      .matchData
      .map(_.group(1))
      .toList match {
      case id :: Nil => id
      case _         => throw new Exception(s"$value is not a valid OrcidId")
    }
  }
  implicit object OrcidId
      extends TinyTypeFactory[OrcidId](new OrcidId(_))
      with EntityIdJsonLdOps[OrcidId]
      with Constraints[OrcidId]
      with NonBlank[OrcidId] {
    private[persons] val validator      = "^https:\\/\\/orcid.org\\/(\\d{4}-\\d{4}-\\d{4}-\\d{4})$"
    private[OrcidId] val validatorRegex = validator.r
    addConstraint(
      check = _.trim.matches(validator),
      message = (v: String) => s"$v is not valid $typeName"
    )
  }

  final class Email private (val value: String) extends AnyVal with StringTinyType
  implicit object Email
      extends TinyTypeFactory[Email](new Email(_))
      with NonBlank[Email]
      with TinyTypeJsonLDOps[Email] {

    addConstraint(
      check = _.split('@').toList match {
        case head +: tail +: Nil => head.trim.nonEmpty && tail.trim.nonEmpty
        case _                   => false
      },
      message = value => s"'$value' is not a valid email"
    )

    implicit class EmailOps(email: Email) {
      lazy val extractName: persons.Name = persons.Name(email.value.split('@').head)
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank[Name] with TinyTypeJsonLDOps[Name]

  final class Username private (val value: String) extends AnyVal with StringTinyType
  implicit object Username extends TinyTypeFactory[Username](new Username(_)) with NonBlank[Username]

  final class Affiliation private (val value: String) extends AnyVal with StringTinyType
  implicit object Affiliation
      extends TinyTypeFactory[Affiliation](new Affiliation(_))
      with NonBlank[Affiliation]
      with TinyTypeJsonLDOps[Affiliation]
}
