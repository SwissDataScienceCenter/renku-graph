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

package io.renku.knowledgegraph.users

import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.rest.paging.PagingRequest
import org.scalacheck.Gen
import projects.Endpoint.Criteria
import projects.Endpoint.Criteria.Filters

package object projects {

  lazy val activationStates: Gen[Filters.ActivationState] = Gen.oneOf(Filters.ActivationState.all)

  private[projects] lazy val criterias: Gen[Criteria] = for {
    userId          <- personGitLabIds
    activationState <- activationStates
    maybeAuthUser   <- authUsers.toGeneratorOfOptions
  } yield Criteria(userId, Filters(activationState), PagingRequest.default, maybeAuthUser)

  private[projects] val activatedProjects: Gen[model.Project.Activated] =
    anyProjectEntities.map(_.to[model.Project.Activated])

  private[projects] val notActivatedProjects: Gen[model.Project.NotActivated] = for {
    id             <- projectIds
    slug           <- projectSlugs
    name           <- projectNames
    visibility     <- projectVisibilities
    dateCreated    <- projectCreatedDates()
    maybeCreatorId <- personGitLabIds.toGeneratorOfOptions
    maybeCreator   <- maybeCreatorId.map(_ => personNames.toGeneratorOfSomes).getOrElse(personNames.toGeneratorOfNones)
    keywords       <- projectKeywords.toGeneratorOfList()
    maybeDesc      <- projectDescriptions.toGeneratorOfOptions
  } yield model.Project.NotActivated(id,
                                     name,
                                     slug,
                                     visibility,
                                     dateCreated,
                                     maybeCreatorId,
                                     maybeCreator,
                                     keywords,
                                     maybeDesc
  )

  private[projects] val modelProjects: Gen[model.Project] = Gen.oneOf(activatedProjects, notActivatedProjects)

  private[projects] implicit def activatedProjectConverter[P <: testentities.Project]: P => model.Project.Activated =
    project =>
      model.Project.Activated(
        project.name,
        project.slug,
        project.visibility,
        project.dateCreated,
        project.maybeCreator.map(_.name),
        project.keywords.toList.sorted,
        project.maybeDescription
      )
}
