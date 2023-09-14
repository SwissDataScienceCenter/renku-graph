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

package io.renku.core.client

import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.graph.model.projects

final case class NewProject(projectRepository: ProjectRepository,
                            namespace:         projects.Namespace,
                            name:              projects.Name,
                            maybeDescription:  Option[projects.Description],
                            keywords:          Set[projects.Keyword],
                            template:          Template,
                            branch:            Branch,
                            userInfo:          UserInfo
)

object NewProject {
  implicit val encoder: Encoder[NewProject] = Encoder.instance {
    case NewProject(projectRepository, namespace, name, maybeDescription, keywords, template, branch, _) =>
      Json.obj(
        List(
          ("url"                -> template.repositoryUrl.asJson).some,
          ("identifier"         -> template.identifier.asJson).some,
          ("project_repository" -> projectRepository.asJson).some,
          ("project_namespace"  -> namespace.asJson).some,
          ("project_name"       -> name.asJson).some,
          ("project_keywords"   -> keywords.asJson).some,
          ("initial_branch"     -> branch.asJson).some,
          maybeDescription.map("project_description" -> _.asJson)
        ).flatten: _*
      )
  }
}
