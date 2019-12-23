/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.http.server

import ch.datascience.graph.model.events.{ProjectId => ProjectIdType}
import ch.datascience.graph.model.projects.{ProjectPath => ProjectPathType}
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

import scala.util.Try

object binders {

  object ProjectId {

    def unapply(value: String): Option[ProjectIdType] =
      Try {
        ProjectIdType(value.toInt)
      }.toOption
  }

  object ProjectPath {

    def unapply(value: String): Option[ProjectPathType] =
      ProjectPathType.from(value).toOption

    class Namespace private (val value: String) extends AnyVal with StringTinyType
    object Namespace {

      private object NamespaceFactory extends TinyTypeFactory[Namespace](new Namespace(_)) with NonBlank

      def unapply(value: String): Option[Namespace] = NamespaceFactory.from(value).toOption

      implicit class NamespaceOps(namespace: Namespace) {
        def /(name: Name): ProjectPathType = ProjectPathType(s"$namespace/$name")
      }
    }

    class Name private (val value: String) extends AnyVal with StringTinyType
    object Name {

      private object NameFactory extends TinyTypeFactory[Name](new Name(_)) with NonBlank

      def unapply(value: String): Option[Name] = NameFactory.from(value).toOption
    }
  }
}
