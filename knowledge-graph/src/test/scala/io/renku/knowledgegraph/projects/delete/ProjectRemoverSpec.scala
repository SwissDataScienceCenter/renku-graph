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

package io.renku.knowledgegraph.projects.delete

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Uri
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectRemoverSpec extends AnyWordSpec with should.Matchers with MockFactory {

  val authUser         = authUsers.generateOne
  private val glClient = mock[GitLabClient[Try]]

  def givenProjectDelete(path: projects.Path, returning: Try[Unit]) = {
    val endpointName: String Refined NonEmpty = "project-delete"
    (glClient
      .delete(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[Try, Unit])(_: Option[AccessToken]))
      .expects(uri"projects" / path, endpointName, *, authUser.accessToken.some)
      .returning(returning)
  }

}
