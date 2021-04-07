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

package ch.datascience.rdfstore.entities

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.Activity._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{BoundedInstant, PositiveInt, UUID}

import java.time.Instant
import scala.language.postfixOps

final case class Activity(id:                  Id,
                          startTime:           StartTime,
                          endTime:             EndTime,
                          author:              Person,
                          agent:               Agent,
                          project:             Project,
                          order:               Order,
                          associationFactory:  Activity => Association,
                          usageFactories:      List[Activity => Usage],
                          generationFactories: List[Activity => Generation],
                          maybeInvalidation:   Option[Entity]
) {

  lazy val association: Association      = associationFactory(this)
  lazy val usages:      List[Usage]      = usageFactories.map(_.apply(this))
  lazy val generations: List[Generation] = generationFactories.map(_.apply(this))

  def entity(location: Location): Entity =
    generations
      .map(_.entity)
      .find(_.location == location)
      .getOrElse(throw new IllegalStateException(s"No entity for $location on Activity for $id"))
}

object Activity {

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID

  final class StartTime private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object StartTime extends TinyTypeFactory[StartTime](new StartTime(_)) with BoundedInstant {
    import java.time.temporal.ChronoUnit.HOURS
    protected[this] override def maybeMax: Option[Instant] = now.plus(24, HOURS).some
  }

  final class EndTime private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object EndTime extends TinyTypeFactory[EndTime](new EndTime(_)) with BoundedInstant {
    import java.time.temporal.ChronoUnit.HOURS
    protected[this] override def maybeMax: Option[Instant] = now.plus(24, HOURS).some
  }

  final class Order private (val value: Int) extends AnyVal with IntTinyType
  implicit object Order extends TinyTypeFactory[Order](new Order(_)) with PositiveInt

  def apply(id:                  Id,
            startTime:           StartTime,
            author:              Person,
            agent:               Agent,
            project:             Project,
            order:               Order,
            associationFactory:  Activity => Association,
            usageFactories:      List[Activity => Usage] = Nil,
            generationFactories: List[Activity => Generation] = Nil,
            maybeInvalidation:   Option[Entity] = None
  ): Activity = new Activity(id,
                             startTime,
                             EndTime(startTime.value),
                             author,
                             agent,
                             project,
                             order,
                             associationFactory,
                             usageFactories,
                             generationFactories,
                             maybeInvalidation
  )

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance { entity =>
      val invalidationReverse = entity.maybeInvalidation
        .map { invalidation =>
          Reverse.ofJsonLDsUnsafe((prov / "wasInvalidatedBy") -> invalidation.asJsonLD)
        }
        .getOrElse(Reverse.empty)

      val generationReverse = Reverse.ofJsonLDsUnsafe((prov / "activity") -> entity.generations.asJsonLD)

      JsonLD.entity(
        entity.asEntityId,
        EntityTypes of (prov / "Activity"),
        invalidationReverse |+| generationReverse,
        prov / "startedAtTime"        -> entity.startTime.asJsonLD,
        prov / "endedAtTime"          -> entity.endTime.asJsonLD,
        prov / "wasAssociatedWith"    -> JsonLD.arr(entity.agent.asJsonLD, entity.author.asJsonLD),
        prov / "qualifiedAssociation" -> entity.association.asJsonLD,
        prov / "qualifiedUsage"       -> entity.usages.asJsonLD,
        schema / "isPartOf"           -> entity.project.asJsonLD,
        renku / "order"               -> entity.order.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Activity] =
    EntityIdEncoder.instance(entity => EntityId of (renkuBaseUrl / "activities" / entity.id))
}
