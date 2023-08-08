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

package io.renku.triplesstore.client.util

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

sealed trait JenaRunMode

object JenaRunMode {

  /** A docker container is started creating a port mapping using the given port. */
  final case class FixedPortContainer(port: Int Refined Positive) extends JenaRunMode

  /** A docker container is started using a random port mapping. */
  case object GenericContainer extends JenaRunMode

  /** No container is started, it is assumed that Jena is running and accepts connections at the given port */
  final case class Local(port: Int Refined Positive) extends JenaRunMode
}
