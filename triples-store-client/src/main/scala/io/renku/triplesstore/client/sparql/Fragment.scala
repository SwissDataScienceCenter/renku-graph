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

package io.renku.triplesstore.client.sparql

import cats.{Monoid, Show}

final case class Fragment(sparql: String)

object Fragment {

  val empty: Fragment = Fragment("")

  implicit val show: Show[Fragment] = Show.show(_.sparql)
  implicit val monoid: Monoid[Fragment] = {

    val cmb: (Fragment, Fragment) => Fragment = {
      case (Fragment.empty, r) => r
      case (l, Fragment.empty) => l
      case (l, r)              => Fragment(s"${l.sparql}\n${r.sparql}")
    }

    Monoid.instance(empty, cmb)
  }
}
