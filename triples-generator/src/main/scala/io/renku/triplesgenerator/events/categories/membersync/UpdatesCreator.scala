/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.membersync

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.config.{GitLabUrlLoader, RenkuBaseUrlLoader}
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model._
import io.renku.graph.model.projects.ResourceId
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.rdfstore.SparqlQuery
import io.renku.rdfstore.SparqlQuery.Prefixes

private class UpdatesCreator(renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl) {

  def insertion(projectPath: projects.Path,
                members:     Set[(GitLabProjectMember, Option[persons.ResourceId])]
  ): List[SparqlQuery] = {
    val (queriesForNewPersons, queriesForExistingPersons) =
      (separateUsersNotYetInKG.pure[List] ap members.toList).separate
        .bimap(createPersonsAndMemberLinks(projectPath, _), createMemberLinks(projectPath, _))
    queriesForNewPersons ::: queriesForExistingPersons
  }

  def removal(projectPath: projects.Path, members: Set[KGProjectMember]): List[SparqlQuery] =
    members
      .map(member => generateLinkTriple(projectPath, member.resourceId))
      .map { linkTriple =>
        SparqlQuery
          .of(
            name = "unlink project member",
            Prefixes of schema -> "schema",
            s"""|DELETE DATA { 
                |  $linkTriple
                |}
                |""".stripMargin
          )
      }
      .toList

  private def createPersonsAndMemberLinks(projectPath:       projects.Path,
                                          membersNotYetInKG: List[GitLabProjectMember]
  ): List[SparqlQuery] = {
    val resourceIdsAndMembers = membersNotYetInKG.map(member => (generateResourceId(member.gitLabId), member))
    val membersCreations = resourceIdsAndMembers.map { resourceIdsAndMember =>
      SparqlQuery.of(
        name = "create project member",
        Prefixes of schema -> "schema",
        s"""|INSERT DATA { 
            |  ${generatePersonTriples(resourceIdsAndMember)}
            |}
            |  """.stripMargin
      )
    }
    val linksCreations =
      resourceIdsAndMembers
        .map { case (resourceId, _) => generateLinkTriple(projectPath, resourceId) }
        .map { linkTriple =>
          SparqlQuery.of(
            name = "link project member",
            Prefixes of schema -> "schema",
            s"""|INSERT DATA { 
                |  $linkTriple
                |}
                |  """.stripMargin
          )
        }
    membersCreations ::: linksCreations
  }

  private def generateResourceId(gitLabId: GitLabId): persons.ResourceId =
    persons.ResourceId(gitLabId)(renkuBaseUrl)

  private def createMemberLinks(projectPath:        projects.Path,
                                membersAlreadyInKG: List[(GitLabProjectMember, persons.ResourceId)]
  ): List[SparqlQuery] =
    membersAlreadyInKG.map { case (_, resourceId) => generateLinkTriple(projectPath, resourceId) }.map { linkTriple =>
      SparqlQuery.of(
        name = "link project member",
        Prefixes of schema -> "schema",
        s"""|INSERT DATA {
            |  $linkTriple
            |}
            |""".stripMargin
      )
    }

  private def generateLinkTriple(projectPath: projects.Path, memberResourceId: persons.ResourceId) =
    s"${ResourceId(projectPath)(renkuBaseUrl).showAs[RdfResource]} schema:member ${memberResourceId.showAs[RdfResource]}."

  private lazy val generatePersonTriples: ((persons.ResourceId, GitLabProjectMember)) => String = {
    case (resourceId, member) =>
      val creatorResource = resourceId.showAs[RdfResource]
      val sameAsId        = entities.Person.toSameAsEntityId(member.gitLabId)(gitLabApiUrl).show
      s"""|$creatorResource a schema:Person;
          |                 schema:name '${sparqlEncode(member.name.value)}';
          |                 schema:sameAs <$sameAsId>.
          |<$sameAsId> a schema:URL;
          |            schema:identifier ${member.gitLabId};
          |            schema:additionalType 'GitLab'.
          |""".stripMargin
  }

  private lazy val separateUsersNotYetInKG: (
      (GitLabProjectMember, Option[persons.ResourceId])
  ) => Either[GitLabProjectMember, (GitLabProjectMember, persons.ResourceId)] = {
    case (member, Some(resourceId)) => Right(member -> resourceId)
    case (member, None)             => Left(member)
  }
}

private object UpdatesCreator {
  def apply[F[_]: MonadThrow]: F[UpdatesCreator] = for {
    renkuBaseUrl <- RenkuBaseUrlLoader[F]()
    gitLabUrl    <- GitLabUrlLoader[F]()
  } yield new UpdatesCreator(renkuBaseUrl, gitLabUrl.apiV4)
}
