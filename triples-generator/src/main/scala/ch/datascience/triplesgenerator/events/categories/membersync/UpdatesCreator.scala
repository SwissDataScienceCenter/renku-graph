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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.graph.config.{GitLabUrlLoader, RenkuBaseUrlLoader}
import ch.datascience.graph.model.Schemas.{rdf, schema}
import ch.datascience.graph.model.projects.ResourceId
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.graph.model.views.SparqlValueEncoder.sparqlEncode
import ch.datascience.graph.model._
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import eu.timepit.refined.auto._

private class UpdatesCreator(renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl) {

  def insertion(
      projectPath: projects.Path,
      members:     Set[(GitLabProjectMember, Option[users.ResourceId])]
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
            Prefixes.of(schema -> "schema", rdf -> "rdf"),
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
        Prefixes.of(schema -> "schema", rdf -> "rdf"),
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
            Prefixes.of(schema -> "schema", rdf -> "rdf"),
            s"""|INSERT DATA { 
                |  $linkTriple
                |}
                |  """.stripMargin
          )
        }
    membersCreations ::: linksCreations
  }

  private def generateResourceId(gitLabId: GitLabId): users.ResourceId =
    users.ResourceId(gitLabId)(renkuBaseUrl)

  private def createMemberLinks(projectPath:        projects.Path,
                                membersAlreadyInKG: List[(GitLabProjectMember, users.ResourceId)]
  ): List[SparqlQuery] =
    membersAlreadyInKG.map { case (_, resourceId) => generateLinkTriple(projectPath, resourceId) }.map { linkTriple =>
      SparqlQuery.of(
        name = "link project member",
        Prefixes.of(schema -> "schema", rdf -> "rdf"),
        s"""|INSERT DATA {
            |  $linkTriple
            |}
            |""".stripMargin
      )
    }

  private def generateLinkTriple(projectPath: projects.Path, memberResourceId: users.ResourceId) =
    s"${ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]} schema:member ${memberResourceId.showAs[RdfResource]}."

  private lazy val generatePersonTriples: ((users.ResourceId, GitLabProjectMember)) => String = {
    case (resourceId, member) =>
      val creatorResource = resourceId.showAs[RdfResource]
      val sameAsId        = (gitLabApiUrl / "users" / member.gitLabId).showAs[RdfResource]
      s"""|  $creatorResource rdf:type <http://schema.org/Person>;
          |                   schema:name '${sparqlEncode(member.name.value)}';
          |                   schema:sameAs $sameAsId.
          |  $sameAsId rdf:type schema:URL;
          |            schema:identifier ${member.gitLabId};
          |            schema:additionalType 'GitLab'.
          |""".stripMargin
  }

  private lazy val separateUsersNotYetInKG: (
      (GitLabProjectMember, Option[users.ResourceId])
  ) => Either[GitLabProjectMember, (GitLabProjectMember, users.ResourceId)] = {
    case (member, Some(resourceId)) => Right(member -> resourceId)
    case (member, None)             => Left(member)
  }
}

private object UpdatesCreator {
  def apply(): IO[UpdatesCreator] = for {
    renkuBaseUrl <- RenkuBaseUrlLoader[IO]()
    gitLabUrl    <- GitLabUrlLoader[IO]()
  } yield new UpdatesCreator(renkuBaseUrl, gitLabUrl.apiV4)
}
