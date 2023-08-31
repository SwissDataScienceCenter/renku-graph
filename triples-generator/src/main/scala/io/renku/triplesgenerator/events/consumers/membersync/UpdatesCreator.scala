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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model._
import io.renku.graph.model.entities.Person
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.views.SparqlLiteralEncoder.sparqlEncode
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes

private class UpdatesCreator(implicit renkuUrl: RenkuUrl, gitLabApiUrl: GitLabApiUrl) {

  def insertion(projectSlug: projects.Slug,
                members:     Set[(GitLabProjectMember, Option[persons.ResourceId])]
  ): List[SparqlQuery] = {
    val (queriesForNewPersons, queriesForExistingPersons) =
      (separateUsersNotYetInKG.pure[List] ap members.toList).separate
        .bimap(createPersonsAndMemberLinks(projectSlug, _), createMemberLinks(projectSlug, _))
    queriesForNewPersons ::: queriesForExistingPersons
  }

  def removal(slug: projects.Slug, members: Set[KGProjectMember]): List[SparqlQuery] = {
    val projectId = projects.ResourceId(slug)
    members
      .map(member => generateEdge(projectId, member.resourceId))
      .map { edge =>
        SparqlQuery
          .of(
            name = "unlink project member",
            Prefixes of schema -> "schema",
            s"""|DELETE DATA { 
                |  GRAPH <${GraphClass.Project.id(projectId)}> { $edge }
                |}
                |""".stripMargin
          )
      }
      .toList
  }

  private def createPersonsAndMemberLinks(slug:              projects.Slug,
                                          membersNotYetInKG: List[GitLabProjectMember]
  ): List[SparqlQuery] = {
    val projectId             = projects.ResourceId(slug)
    val resourceIdsAndMembers = membersNotYetInKG.map(member => (generateResourceId(member.gitLabId), member))
    val membersCreations = resourceIdsAndMembers.map { resourceIdsAndMember =>
      SparqlQuery.of(
        name = "create project member",
        Prefixes of schema -> "schema",
        s"""|INSERT DATA {
            |  GRAPH <${GraphClass.Persons.id}> {
            |    ${generatePersonTriples(resourceIdsAndMember)}
            |  }
            |}
            |  """.stripMargin
      )
    }
    val linksCreations =
      resourceIdsAndMembers
        .map { case (memberId, _) => generateEdge(projectId, memberId) }
        .map { edge =>
          SparqlQuery.of(
            name = "link project member",
            Prefixes of schema -> "schema",
            s"""|INSERT DATA { 
                |  GRAPH <${GraphClass.Project.id(projectId)}> { $edge }
                |}
                |  """.stripMargin
          )
        }
    membersCreations ::: linksCreations
  }

  private def generateResourceId(gitLabId: GitLabId): persons.ResourceId =
    persons.ResourceId(gitLabId)(renkuUrl)

  private def createMemberLinks(slug:               projects.Slug,
                                membersAlreadyInKG: List[(GitLabProjectMember, persons.ResourceId)]
  ): List[SparqlQuery] = {
    val projectId = projects.ResourceId(slug)
    membersAlreadyInKG
      .map { case (_, resourceId) => generateEdge(projectId, resourceId) }
      .map { edge =>
        SparqlQuery.of(
          name = "link project member",
          Prefixes of schema -> "schema",
          s"""|INSERT DATA {
              |  GRAPH <${GraphClass.Project.id(projectId)}> { $edge }
              |}
              |""".stripMargin
        )
      }
  }

  private def generateEdge(projectId: projects.ResourceId, personId: persons.ResourceId) =
    s"${projectId.showAs[RdfResource]} schema:member ${personId.showAs[RdfResource]}."

  private lazy val generatePersonTriples: ((persons.ResourceId, GitLabProjectMember)) => String = {
    case (resourceId, member) =>
      val creatorResource = resourceId.showAs[RdfResource]
      val sameAsId        = entities.Person.toGitLabSameAsEntityId(member.gitLabId)(gitLabApiUrl).show
      s"""|$creatorResource a schema:Person;
          |                 schema:name '${sparqlEncode(member.name.value)}';
          |                 schema:sameAs <$sameAsId>.
          |<$sameAsId> a schema:URL;
          |            schema:identifier ${member.gitLabId};
          |            schema:additionalType '${Person.gitLabSameAsAdditionalType}'.
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
    implicit0(renkuUrl: RenkuUrl)      <- RenkuUrlLoader[F]()
    implicit0(gitLabUrl: GitLabApiUrl) <- GitLabUrlLoader[F]().map(_.apiV4)
  } yield new UpdatesCreator
}
