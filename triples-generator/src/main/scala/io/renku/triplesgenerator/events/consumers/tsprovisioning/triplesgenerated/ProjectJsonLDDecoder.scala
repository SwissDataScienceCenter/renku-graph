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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesgenerated

import cats.syntax.all._
import io.renku.cli.model.CliProject
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.entities.Project
import io.renku.graph.model.entities.Project.GitLabProjectInfo
import io.renku.jsonld.JsonLDDecoder

trait ProjectJsonLDDecoder {

  def apply(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): JsonLDDecoder[Project] =
    CliProject.projectAndPersonDecoder.emap { case (project, persons) =>
      Project.fromCli(project, persons.toSet, gitLabInfo).toEither.leftMap(_.intercalate("; "))
    }

  def list(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): JsonLDDecoder[List[Project]] =
    JsonLDDecoder.decodeList(apply(gitLabInfo))
}

object ProjectJsonLDDecoder extends ProjectJsonLDDecoder
