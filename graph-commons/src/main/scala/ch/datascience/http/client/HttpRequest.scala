/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.client

import cats.effect.IO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import org.http4s.Request

sealed trait HttpRequest {
  def request: Request[IO]
}

object HttpRequest {

  def apply(request: Request[IO]): UnnamedRequest = UnnamedRequest(request)
  def apply(request: Request[IO], name: String Refined NonEmpty): NamedRequest = NamedRequest(request, name)

  final case class UnnamedRequest(request: Request[IO]) extends HttpRequest
  final case class NamedRequest(request:   Request[IO], name: String Refined NonEmpty) extends HttpRequest
}
