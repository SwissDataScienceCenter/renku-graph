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

package io.renku.graph.model.tools

import io.renku.graph.model.tools.JsonLDTools.JsonLDElementView.{Filter, Update}
import io.renku.jsonld._

object JsonLDTools {

  def flattenedJsonLD[A: JsonLDEncoder](value: A): JsonLD =
    JsonLDEncoder[A].apply(value).flatten.fold(throw _, identity)

  def flattenedJsonLDFrom(value: JsonLD, other: JsonLD*): JsonLD =
    JsonLD.arr(value :: other.toList: _*).flatten.fold(throw _, identity)

  /** Create a view of the value as JsonLD in order to create a modified version. */
  def view[A: JsonLDEncoder](value: A): JsonLDElementView =
    view(flattenedJsonLD(value))

  /** Create a view of the value as JsonLD in order to create a modified version. */
  def view(jsonld: JsonLD): JsonLDElementView =
    JsonLDElementView(jsonld.asArray.getOrElse(Seq.empty), Filter.all, Update.none)

  trait JsonLDElementView {

    /** Select elements that have all the given types. */
    def selectByTypes(ets: EntityTypes): JsonLDElementView

    /** Add to the current elements a new type. */
    def addType(et: EntityType): JsonLDElementView

    final def addType(et: Property): JsonLDElementView = addType(EntityType.of(et))

    /** Remove the given property. */
    def remove(ets: EntityTypes, property: Property): JsonLDElementView

    def value: JsonLD
  }

  object JsonLDElementView {
    type Filter = JsonLD => Boolean
    type Update = JsonLD => JsonLD

    /** Operate on a list of JsonLD elements by applying updates to a selected list of elements. */
    def apply(root: Seq[JsonLD], filter: Filter, update: Update): JsonLDElementView =
      new JsonLDElementView {
        val value: JsonLD =
          JsonLD.JsonLDArray(root.map { el =>
            if (filter(el)) update(el) else el
          })

        override def selectByTypes(ets: EntityTypes): JsonLDElementView =
          apply(root, filter && Filter.containsAllTypes(ets), update)

        override def addType(et: EntityType): JsonLDElementView =
          JsonLDElementView(root, filter, update.andThen(Update.addEntityType(et)))

        override def remove(ets: EntityTypes, property: Property): JsonLDElementView =
          JsonLDElementView(root,
                            filter && Filter.containsAllTypes(ets),
                            update.andThen(Update.removeProperty(property))
          )
      }

    object Update {
      val none: Update = identity

      def addEntityType(et: EntityType): JsonLD => JsonLD = {
        case e: JsonLD.JsonLDEntity =>
          e.copy(types = EntityTypes(e.types.list.prepend(et)))
        case e => e
      }

      def removeProperty(property: Property): JsonLD => JsonLD = {
        case e: JsonLD.JsonLDEntity => e.copy(properties = e.properties - property)
        case e => e
      }
    }

    object Filter {
      val all: Filter = _ => true

      def containsAllTypes(ets: EntityTypes): Filter =
        el => el.entityTypes.exists(_.contains(ets))
    }

    implicit class FilterOps(self: Filter) {
      def &&(other: Filter): Filter =
        a => self(a) && other(a)

      def ||(other: Filter): Filter =
        a => self(a) || other(a)
    }
  }
}
