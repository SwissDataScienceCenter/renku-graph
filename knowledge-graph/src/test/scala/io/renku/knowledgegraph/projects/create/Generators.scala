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

package io.renku.knowledgegraph.projects.create

import cats.syntax.all._
import io.renku.core.client.Generators.templatesGen
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.knowledgegraph.projects.images.ImageGenerators
import org.scalacheck.Gen

private object Generators {

  implicit val namespaceIds:     Gen[NamespaceId]  = positiveInts().map(_.value).toGeneratorOf(NamespaceId)
  implicit val namespacesIdOnly: Gen[Namespace.Id] = namespaceIds.map(Namespace.apply)
  implicit val namespaces: Gen[Namespace.WithName] = (namespaceIds, projectNamespaces).mapN(Namespace.WithName.apply)
  def namespacesWithName(from: Namespace): Gen[Namespace.WithName] =
    projectNamespaces.map(Namespace.WithName(from.identifier, _))

  implicit val newProjects: Gen[NewProject] =
    (projectNames,
     namespacesIdOnly,
     projectDescriptions.toGeneratorOfOptions,
     projectKeywords.toGeneratorOfSet(),
     projectVisibilities,
     templatesGen,
     ImageGenerators.images.toGeneratorOfOptions
    ).mapN(NewProject.apply)

  implicit val glCreatedProjectCreatorsGen: Gen[GLCreatedProject.Creator] =
    (personNames, personGitLabIds).mapN(GLCreatedProject.Creator.apply)

  implicit val glCreatedProjectsGen: Gen[GLCreatedProject] =
    (projectIds, projectSlugs, projectCreatedDates(), glCreatedProjectCreatorsGen, imageUris.toGeneratorOfOptions)
      .mapN(GLCreatedProject.apply)
}
