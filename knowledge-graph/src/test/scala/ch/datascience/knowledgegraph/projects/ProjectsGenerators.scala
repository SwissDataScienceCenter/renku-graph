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

package ch.datascience.knowledgegraph.projects

import ch.datascience.generators.CommonGraphGenerators.{emails, names}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.projects.model._
import org.scalacheck.Gen

object ProjectsGenerators {

  implicit val projects: Gen[Project] = for {
    id      <- projectPaths
    name    <- projectNames
    created <- projectCreations
  } yield Project(id, name, created)

  private implicit lazy val projectCreations: Gen[ProjectCreation] = for {
    created <- projectCreatedDates
    creator <- projectCreators
  } yield ProjectCreation(created, creator)

  private implicit lazy val projectCreators: Gen[ProjectCreator] = for {
    email <- emails
    name  <- names
  } yield ProjectCreator(email, name)
}
