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

package io.renku.graph.model.testentities

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{GitLabApiUrl, GraphClass, InvalidationTime, RenkuUrl, entities, plans}
import io.renku.graph.model.plans.{Command, DateCreated, DerivedFrom, Description, Identifier, Keyword, Name}
import io.renku.jsonld.JsonLDEncoder
import io.renku.jsonld.syntax._

sealed trait CompositePlan extends Plan {
  override type PlanGroup         = CompositePlan
  override type PlanGroupModified = CompositePlan.Modified

  def plans: NonEmptyList[Plan]

  def links: List[ParameterLink]

  def addLink(link: ParameterLink): PlanType

  def resetLinks: PlanType

  def mappings: List[ParameterMapping]

  def addParamMapping(pm: ParameterMapping): PlanType

  def resetMappings: PlanType

  def modify(f: PlanType => PlanType): PlanType

  final def widen: Plan = this
}

object CompositePlan {

  final case class NonModified(
      id:               Identifier,
      name:             Name,
      maybeDescription: Option[Description],
      creators:         List[Person],
      dateCreated:      DateCreated,
      keywords:         List[Keyword],
      plans:            NonEmptyList[Plan],
      mappings:         List[ParameterMapping],
      links:            List[ParameterLink]
  ) extends CompositePlan {
    override type PlanType = NonModified

    def modify(f: NonModified => NonModified): NonModified =
      f(this)

    def addLink(link: ParameterLink): NonModified =
      copy(links = link :: links.filterNot(_.id == link.id))

    def addParamMapping(pm: ParameterMapping): NonModified =
      copy(mappings = pm :: mappings.filterNot(_.id == pm.id))

    def resetLinks: NonModified = copy(links = Nil)

    def resetMappings: NonModified = copy(mappings = Nil)

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
        entities.CompositePlan
          .validate(
            entities.CompositePlan.NonModified(
              resourceId = plans.ResourceId(plan.asEntityId.show),
              name = plan.name,
              maybeDescription = plan.maybeDescription,
              creators = plan.creators.map(_.to[entities.Person]),
              dateCreated = plan.dateCreated,
              keywords = plan.keywords,
              plans = plan.plans.map(p => Plan.toEntitiesPlan.apply(p).resourceId),
              mappings = plan.mappings.map(ParameterMapping.toEntitiesParameterMapping),
              links = plan.links.map(ParameterLink.toEntitiesParameterLink)
            )
          )
          .fold(errors => sys.error(errors.intercalate("; ")), _.asInstanceOf[entities.CompositePlan.NonModified])
  }

  case class Modified(
      id:               Identifier,
      name:             Name,
      maybeDescription: Option[Description],
      creators:         List[Person],
      dateCreated:      DateCreated,
      keywords:         List[Keyword],
      plans:            NonEmptyList[Plan],
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

    def modify(f: Modified => Modified): Modified =
      f(this)

    def addLink(link: ParameterLink): Modified =
      copy(links = link :: links.filterNot(_.id == link.id))

    def addParamMapping(pm: ParameterMapping): Modified =
      copy(mappings = pm :: mappings.filterNot(_.id == pm.id))

    def resetLinks: Modified = copy(links = Nil)

    def resetMappings: Modified = copy(mappings = Nil)

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
        entities.CompositePlan
          .validate(
            entities.CompositePlan.Modified(
              resourceId = plans.ResourceId(plan.asEntityId.show),
              name = plan.name,
              maybeDescription = plan.maybeDescription,
              creators = plan.creators.map(_.to[entities.Person]),
              dateCreated = plan.dateCreated,
              keywords = plan.keywords,
              plans = plan.plans.map(p => Plan.toEntitiesPlan.apply(p).resourceId),
              mappings = plan.mappings.map(ParameterMapping.toEntitiesParameterMapping),
              links = plan.links.map(ParameterLink.toEntitiesParameterLink),
              maybeInvalidationTime = plan match {
                case p: HavingInvalidationTime => p.invalidationTime.some
                case _ => None
              },
              derivation = entities.Plan.Derivation(
                DerivedFrom(plan.parent.asEntityId),
                plans.ResourceId(plan.topmostParent.asEntityId.show)
              )
            )
          )
          .fold(errors => sys.error(errors.intercalate("; ")), _.asInstanceOf[entities.CompositePlan.Modified])
  }

  implicit def toEntitiesCompositePlan(implicit renkuUrl: RenkuUrl): CompositePlan => entities.CompositePlan = {
    case p: NonModified => NonModified.toEntitiesCompositePlan(renkuUrl)(p)
    case p: Modified    => Modified.toEntitiesCompositePlan(renkuUrl)(p)
  }

  // maybe just use the project encoder on the production entities
  implicit def jsonLDEncoder(implicit
      renkuUrl:     RenkuUrl,
      gitLabApiUrl: GitLabApiUrl,
      graphClass:   GraphClass
  ): JsonLDEncoder[CompositePlan] =
    // we need to serialize the child plans completely and not only their ids
    JsonLDEncoder.instance { cp =>
      val children = cp.plans.map(_.asJsonLD)
      (cp.to[entities.CompositePlan].asJsonLD :: children).asJsonLD
    }
}
