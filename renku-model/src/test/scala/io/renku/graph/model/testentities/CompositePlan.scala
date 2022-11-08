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

package io.renku.graph.model.testentities

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{InvalidationTime, RenkuUrl, commandParameters, entities, parameterLinks, plans}
import io.renku.graph.model.plans.{Command, DateCreated, DerivedFrom, Description, Identifier, Keyword, Name, ResourceId}
import io.renku.jsonld.syntax._

sealed trait CompositePlan extends Plan {
  override type PlanGroup         = CompositePlan
  override type PlanGroupModified = CompositePlan.Modified
}

object CompositePlan {
  final case class ParameterMapping(
      id:               Identifier,
      defaultValue:     ParameterMapping.DefaultValue,
      maybeDescription: Option[commandParameters.Description],
      name:             commandParameters.Name,
      mappedParameter:  NonEmptyList[Identifier]
  ) {
    def toEntitiesParameterMapping: entities.ParameterMapping =
      entities.ParameterMapping(
        resourceId = commandParameters.ResourceId(id.value),
        defaultValue = entities.ParameterMapping.DefaultValue(defaultValue),
        maybeDescription = maybeDescription,
        name = name,
        mappedParameter = mappedParameter.map(id => commandParameters.ResourceId(id.value))
      )
  }

  object ParameterMapping {
    type DefaultValue = String

    def apply(id:           Identifier,
              defaultValue: DefaultValue,
              description:  Option[String],
              name:         String,
              mappedParam:  Identifier,
              mappedParams: Identifier*
    ): ParameterMapping =
      ParameterMapping(
        id,
        defaultValue,
        description.map(commandParameters.Description.apply),
        commandParameters.Name(name),
        NonEmptyList(mappedParam, mappedParams.toList)
      )
  }

  final case class ParameterLink(id: Identifier, source: Identifier, sinks: NonEmptyList[Identifier]) {
    def toEntitiesMapping: entities.ParameterLink =
      entities.ParameterLink(
        resourceId = parameterLinks.ResourceId(id.value),
        source = commandParameters.ResourceId(source.value),
        sinks = sinks.map(id => commandParameters.ResourceId(id.value))
      )
  }

  object ParameterLink {
    def apply(id: Identifier, source: Identifier, sink: Identifier, sinks: Identifier*): ParameterLink =
      ParameterLink(id, source, NonEmptyList(sink, sinks.toList))
  }

  final case class NonModified(
      id:               Identifier,
      name:             Name,
      maybeDescription: Option[Description],
      creators:         List[Person],
      dateCreated:      DateCreated,
      keywords:         List[Keyword],
      plans:            NonEmptyList[Identifier],
      mappings:         List[ParameterMapping],
      links:            List[ParameterLink]
  ) extends CompositePlan {
    override type PlanType = NonModified

    def addLink(link: ParameterLink): NonModified =
      copy(links = link :: links.filterNot(_.id == link.id))

    def addParamMapping(pm: ParameterMapping): NonModified =
      copy(mappings = pm :: mappings.filterNot(_.id == pm.id))

    override def to[T](implicit convert: NonModified => T): T = convert(this)

    override def createModification(f: NonModified => NonModified): Modified =
      Modified(id, name, maybeDescription, creators, dateCreated, keywords, plans, mappings, links, this)

    override def invalidate(time: InvalidationTime): ValidatedNel[String, Modified with HavingInvalidationTime] =
      Validated.invalidNel("Not a modified plan")

    override def replaceCreators(creators: List[Person]): NonModified =
      copy(creators = creators)

    override def replacePlanName(to: Name): NonModified =
      copy(name = to)

    override def replaceCommand(to: Option[Command]): NonModified = this

    override def replacePlanKeywords(to: List[Keyword]): NonModified =
      copy(keywords = to)

    override def replacePlanDesc(to: Option[Description]): NonModified =
      copy(maybeDescription = to)

    override def replacePlanDateCreated(to: DateCreated): NonModified =
      copy(dateCreated = to)
  }

  object NonModified {
    def toEntitiesCompositePlan[P <: NonModified](implicit
        renkuUrl: RenkuUrl
    ): P => entities.CompositePlan.NonModified =
      plan =>
        entities.CompositePlan.NonModified(
          resourceId = plans.ResourceId(plan.asEntityId.show),
          name = plan.name,
          maybeDescription = plan.maybeDescription,
          creators = plan.creators.map(_.to[entities.Person]),
          dateCreated = plan.dateCreated,
          keywords = plan.keywords,
          plans = plan.plans.map(cid => ResourceId(cid)),
          mappings = plan.mappings.map(_.toEntitiesParameterMapping),
          links = plan.links.map(_.toEntitiesMapping)
        )
  }

  case class Modified(
      id:               Identifier,
      name:             Name,
      maybeDescription: Option[Description],
      creators:         List[Person],
      dateCreated:      DateCreated,
      keywords:         List[Keyword],
      plans:            NonEmptyList[Identifier],
      mappings:         List[ParameterMapping],
      links:            List[ParameterLink],
      parent:           CompositePlan
  ) extends CompositePlan
      with Plan.Modified {
    override type PlanType   = Modified
    override type ParentType = CompositePlan

    lazy val topmostParent: CompositePlan = parent match {
      case p: NonModified => p
      case p: Modified    => p.topmostParent
    }

    override def to[T](implicit convert: Modified => T): T = convert(this)

    override def createModification(f: Modified => Modified): Modified =
      f(this).copy(parent = this)

    override def invalidate(time: InvalidationTime): ValidatedNel[String, Modified with HavingInvalidationTime] =
      validate(dateCreated, time).map(time =>
        new Modified(
          planIdentifiers.generateOne,
          name,
          maybeDescription,
          creators,
          dateCreated,
          keywords,
          plans,
          mappings,
          links,
          parent
        ) with HavingInvalidationTime {
          override val invalidationTime: InvalidationTime = time
        }
      )

    override def replaceCreators(creators: List[Person]): Modified =
      copy(creators = creators)

    override def replacePlanName(to: Name): Modified =
      copy(name = to)

    override def replaceCommand(to: Option[Command]): Modified = this

    override def replacePlanKeywords(to: List[Keyword]): Modified =
      copy(keywords = to)

    override def replacePlanDesc(to: Option[Description]): Modified =
      copy(maybeDescription = to)

    override def replacePlanDateCreated(to: DateCreated): Modified =
      copy(dateCreated = to)
  }
  object Modified {
    def toEntitiesCompositePlan(implicit
        renkuUrl: RenkuUrl
    ): Modified => entities.CompositePlan.Modified =
      plan =>
        entities.CompositePlan.Modified(
          resourceId = plans.ResourceId(plan.asEntityId.show),
          name = plan.name,
          maybeDescription = plan.maybeDescription,
          creators = plan.creators.map(_.to[entities.Person]),
          dateCreated = plan.dateCreated,
          keywords = plan.keywords,
          plans = plan.plans.map(id => plans.ResourceId(id)),
          mappings = plan.mappings.map(_.toEntitiesParameterMapping),
          links = plan.links.map(_.toEntitiesMapping),
          maybeInvalidationTime = plan match {
            case p: HavingInvalidationTime => p.invalidationTime.some
            case _ => None
          },
          derivation = entities.Plan.Derivation(
            DerivedFrom(plan.parent.asEntityId),
            plans.ResourceId(plan.topmostParent.asEntityId.show)
          )
        )
  }

  implicit def toEntitiesCompositePlan(implicit renkuUrl: RenkuUrl): CompositePlan => entities.CompositePlan = {
    case p: NonModified => NonModified.toEntitiesCompositePlan(renkuUrl)(p)
    case p: Modified    => Modified.toEntitiesCompositePlan(renkuUrl)(p)
  }
}
