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

package io.renku.http.client

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import org.http4s.Request

sealed trait HttpRequest[F[_]] {
  def request: Request[F]
}

object HttpRequest {

  def apply[F[_]](request: Request[F]): UnnamedRequest[F] = UnnamedRequest(
    request
  )
  def apply[F[_]](request: Request[F], name: String Refined NonEmpty): NamedRequest[F] =
    NamedRequest(request, name)

  final case class UnnamedRequest[F[_]](request: Request[F])                              extends HttpRequest[F]
  final case class NamedRequest[F[_]](request: Request[F], name: String Refined NonEmpty) extends HttpRequest[F]
}
