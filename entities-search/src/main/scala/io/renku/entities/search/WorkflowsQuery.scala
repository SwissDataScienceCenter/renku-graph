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

package io.renku.entities.search

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.entities.search.Criteria.Filters.EntityType
import io.renku.entities.search.model.Entity.Workflow.WorkflowType
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.graph.model.entities.{CompositePlan, StepPlan}
import io.renku.graph.model.{plans, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import io.renku.triplesstore.client.syntax._

private case object WorkflowsQuery extends EntityQuery[model.Entity.Workflow] {

  override val entityType: EntityType = EntityType.Workflow

  override val selectVariables = Set("?entityType",
                                     "?matchingScore",
                                     "?wkId",
                                     "?name",
                                     "?date",
                                     "?maybeDescription",
                                     "?keywords",
                                     "?projectIdVisibilities",
                                     "?workflowTypes"
  )

  private val authSnippets = SparqlSnippets(VarName("projectId"))

  override def query(criteria: Criteria) = (criteria.filters whenRequesting entityType) {
    import criteria._
    // format: off
    sparql"""|{
        |  SELECT ?entityType ?matchingScore ?wkId ?name ?date ?maybeDescription ?keywords ?projectIdVisibilities ?workflowTypes
        |  WHERE {
        |    {
        |      SELECT ?matchingScore ?wkId ?name ?date ?maybeDescription
        |        (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |        (GROUP_CONCAT(DISTINCT ?projectIdVisibility; separator=',') AS ?projectIdVisibilities)
        |        (GROUP_CONCAT(DISTINCT ?workflowType; separator=',') AS ?workflowTypes)
        |      WHERE {
        |        {
        |          SELECT ?wkId ?projectId ?matchingScore
        |            (GROUP_CONCAT(DISTINCT ?childProjectId; separator='|') AS ?childProjectsIds)
        |            (GROUP_CONCAT(DISTINCT ?projectIdWhereInvalidated; separator='|') AS ?projectsIdsWhereInvalidated)
        |          WHERE {
        |            ${filters.foldQuery(withQuerySnippet, withoutQuerySnippet)}
        |            
        |            OPTIONAL {
        |              GRAPH ?childProjectId {
        |                ?childWkId prov:wasDerivedFrom ?wkId;
        |                           ^renku:hasPlan ?childProjectId
        |              }
        |            }
        |            OPTIONAL {
        |              GRAPH ?projectIdWhereInvalidated {
        |                ?wkId prov:invalidatedAtTime ?invalidationTime;
        |                      ^renku:hasPlan ?projectIdWhereInvalidated
        |              }
        |            }
        |          }
        |          GROUP BY ?wkId ?projectId ?matchingScore
        |        }
        |
        |        FILTER (IF (BOUND(?childProjectsIds), !CONTAINS(STR(?childProjectsIds), STR(?projectId)), true))
        |        FILTER (IF (BOUND(?projectsIdsWhereInvalidated), !CONTAINS(STR(?projectsIdsWhereInvalidated), STR(?projectId)), true))
        |
        |        ${accessRightsAndVisibility(criteria.maybeUser, criteria.filters.visibilities)}
        |
        |        GRAPH ?projectId {
        |          ?wkId a prov:Plan;
        |                a ?workflowType;
        |                schema:name ?name;
        |                schema:dateCreated ?date;
        |                ^renku:hasPlan ?projectId.
        |          ?projectId renku:projectNamespace ?namespace.
        |          ?projectId renku:projectVisibility ?visibility.
        |          BIND (CONCAT(STR(?projectId), STR('::'), STR(?visibility)) AS ?projectIdVisibility)
        |          ${filters.maybeOnNamespace(VarName("namespace"))}
        |          ${filters.maybeOnDateCreated(VarName("date"))}
        |          OPTIONAL { ?wkId schema:description ?maybeDescription }
        |          OPTIONAL { ?wkId schema:keywords ?keyword }
        |        }
        |      }
        |      GROUP BY ?wkId ?matchingScore ?name ?date ?maybeDescription
        |    }
        |    BIND ('workflow' AS ?entityType)
        |  }
        |}
        |""".stripMargin.sparql
    // format: on
  }

  private def accessRightsAndVisibility(maybeUser: Option[AuthUser], visibilities: Set[projects.Visibility]): Fragment =
    authSnippets.visibleProjects(maybeUser.map(_.id), visibilities)

  private def withoutQuerySnippet: Fragment =
    sparql"""
            |BIND (xsd:float(1.0) AS ?matchingScore)
            |GRAPH ?projectId {
            |   ?wkId a prov:Plan;
            |         ^renku:hasPlan ?projectId.
            |}
            |""".stripMargin

  def withQuerySnippet(query: LuceneQuery) =
    sparql"""
            |{
            |  SELECT ?wkId (MAX(?score) AS ?matchingScore) (SAMPLE(?projId) AS ?projectId)
            |  WHERE {
            |    (?wkId ?score) text:query (schema:name schema:keywords schema:description $query).
            |    GRAPH ?g {
            |      ?wkId a prov:Plan;
            |            ^renku:hasPlan ?projId
            |      }
            |    }
            |    GROUP BY ?wkId
            |}
            |""".stripMargin

  override def decoder[EE >: Entity.Workflow]: Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import cats.syntax.all._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val toListOfProjectIdsAndVisibilities
        : Option[String] => Decoder.Result[NonEmptyList[(projects.ResourceId, projects.Visibility)]] =
      _.map(
        _.split(",")
          .map(_.trim)
          .map { case s"$projectId::$visibility" =>
            (projects.ResourceId.from(projectId) -> projects.Visibility.from(visibility)).mapN((_, _))
          }
          .toList
          .sequence
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
          .map {
            case head :: tail => NonEmptyList.of(head, tail: _*).some
            case Nil          => None
          }
      ).getOrElse(Option.empty[NonEmptyList[(projects.ResourceId, projects.Visibility)]].asRight)
        .flatMap {
          case Some(tuples) => tuples.asRight
          case None         => DecodingFailure("Plan's project and visibility not found", Nil).asLeft
        }

    lazy val selectBroaderVisibility: NonEmptyList[(projects.ResourceId, projects.Visibility)] => projects.Visibility =
      list =>
        list
          .find(_._2 == projects.Visibility.Public)
          .orElse(list.find(_._2 == projects.Visibility.Internal))
          .map(_._2)
          .getOrElse(projects.Visibility.Private)

    for {
      matchingScore <- extract[MatchingScore]("matchingScore")
      name          <- extract[plans.Name]("name")
      dateCreated   <- extract[plans.DateCreated]("date")
      visibility <- extract[Option[String]]("projectIdVisibilities")
                      .flatMap(toListOfProjectIdsAndVisibilities)
                      .map(selectBroaderVisibility)
      keywords <- extract[Option[String]]("keywords") >>= toListOf[plans.Keyword, plans.Keyword.type](plans.Keyword)
      maybeDescription <- extract[Option[plans.Description]]("maybeDescription")
      workflowTypes    <- extract[WorkflowType]("workflowTypes")
    } yield Entity.Workflow(matchingScore, name, visibility, dateCreated, keywords, maybeDescription, workflowTypes)
  }

  def workflowTypeFromString(str: String): Either[String, WorkflowType] =
    str match {
      case _ if str.contains(CompositePlan.Ontology.typeDef.clazz.id.show) =>
        Right(WorkflowType.Composite)
      case _ if str.contains(StepPlan.ontology.clazz.id.show) =>
        Right(WorkflowType.Step)
      case _ =>
        Left(s"Unknown workflowType value: $str")
    }

  implicit val workflowTypeDecoder: Decoder[WorkflowType] =
    Decoder.decodeString.emap(workflowTypeFromString)
}
