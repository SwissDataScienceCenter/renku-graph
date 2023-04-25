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

package io.renku.knowledgegraph.projects.files.lineage

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{projects, GraphClass, RenkuUrl}
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects.ResourceId
import io.renku.jsonld.syntax._
import io.renku.tinytypes.json.TinyTypeDecoders
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import model.{ExecutionInfo, Node}
import org.typelevel.log4cats.Logger

private trait NodeDetailsFinder[F[_]] {

  def findDetails[T](
      location:    Set[T],
      projectPath: projects.Path
  )(implicit query: (T, ResourceId) => SparqlQuery): F[Set[Node]]
}

private class NodeDetailsFinderImpl[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder](
    storeConfig: ProjectsConnectionConfig
)(implicit renkuUrl: RenkuUrl)
    extends TSClientImpl[F](storeConfig)
    with NodeDetailsFinder[F] {

  override def findDetails[T](
      ids:         Set[T],
      projectPath: projects.Path
  )(implicit query: (T, ResourceId) => SparqlQuery): F[Set[Node]] =
    ids.toList
      .map { id =>
        queryExpecting[Option[Node]](selectQuery = query(id, ResourceId(projectPath)))
          .flatMap(failIf(no = id))
      }
      .parSequence
      .map(_.toSet)

  private implicit val nodeDecoder: Decoder[Option[Node]] = {
    implicit val locationDecoder: Decoder[Node.Location] = TinyTypeDecoders.stringDecoder(Node.Location)
    implicit val labelDecoder:    Decoder[Node.Label]    = TinyTypeDecoders.stringDecoder(Node.Label)

    implicit lazy val fieldsDecoder: Decoder[Option[(Node.Location, String, Node.Label)]] = { cursor =>
      for {
        maybeLocation <- cursor.downField("location").downField("value").as[Option[Node.Location]]
        maybeNodeType <- cursor.downField("type").downField("value").as[Option[String]]
        maybeLabel    <- cursor.downField("label").downField("value").as[Option[Node.Label]].map(trimValue)
      } yield (maybeLocation, maybeNodeType, maybeLabel) mapN ((_, _, _))
    }

    lazy val trimValue: Option[Node.Label] => Option[Node.Label] = _.map(l => Node.Label(l.value.trim))

    lazy val maybeToNode: List[(Node.Location, String, Node.Label)] => Either[DecodingFailure, Option[Node]] = {
      case Nil => Option.empty.asRight
      case (location, typ, label) :: tail =>
        tail.foldLeft((location, Set(typ), label)) {
          case (node, (`location`, t, `label`)) => (location, node._2 + t, label)
          case (node, _)                        => node
        } match {
          case (`location`, types, `label`) =>
            Node.Type
              .fromEntityTypes(types)
              .bimap(err => DecodingFailure(err.getMessage, Nil), Node(location, label, _).some)
        }
    }

    _.downField("results")
      .downField("bindings")
      .as[List[Option[(Node.Location, String, Node.Label)]]]
      .map(_.flatten)
      .flatMap(maybeToNode)
  }

  private def failIf[T](no: T): Option[Node] => F[Node] = {
    case Some(details) => details.pure[F]
    case _ =>
      no match {
        case location: Node.Location =>
          new IllegalArgumentException(s"No entity with $location").raiseError[F, Node]
        case runInfo: ExecutionInfo =>
          new IllegalArgumentException(s"No activity with ${runInfo.entityId}").raiseError[F, Node]
        case other =>
          new IllegalArgumentException(s"Entity $other not recognisable").raiseError[F, Node]
      }
  }
}

private object NodeDetailsFinder {

  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder]: F[NodeDetailsFinder[F]] = for {
    config                        <- ProjectsConnectionConfig[F]()
    implicit0(renkuUrl: RenkuUrl) <- RenkuUrlLoader[F]()
  } yield new NodeDetailsFinderImpl[F](config)

  implicit val locationQuery: (Node.Location, ResourceId) => SparqlQuery = (location, projectId) =>
    SparqlQuery.of(
      name = "lineage - entity details",
      Prefixes of (prov -> "prov", schema -> "schema", renku -> "renku"),
      sparql"""|SELECT DISTINCT ?type ?location ?label
               |WHERE {
               |  {
               |    GRAPH ${GraphClass.Project.id(projectId)} {
               |      ?entity prov:atLocation ${location.asObject};
               |              a prov:Entity;
               |              a ?type
               |    }
               |  } {
               |    GRAPH ${GraphClass.Project.id(projectId)} {
               |      ?activityId a prov:Activity;
               |                  prov:qualifiedUsage / prov:entity ?entity;
               |                  ^renku:hasActivity ${projectId.asEntityId}
               |    }
               |  } UNION {
               |    GRAPH ${GraphClass.Project.id(projectId)} {
               |      ?entity prov:qualifiedGeneration / prov:activity / ^renku:hasActivity ${projectId.asEntityId}
               |    }
               |  }
               |  BIND (${location.asObject} AS ?location)
               |  BIND (${location.asObject} AS ?label)
               |}
               |""".stripMargin
    )

  implicit val activityIdQuery: (ExecutionInfo, ResourceId) => SparqlQuery = {
    case (ExecutionInfo(activityId, _), projectId) =>
      SparqlQuery.of(
        name = "lineage - plan details",
        Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
        sparql"""|SELECT DISTINCT ?type (CONCAT(STR(?command), (GROUP_CONCAT(?commandParameter; separator=' '))) AS ?label) ?location
                 |WHERE {
                 |  {
                 |    SELECT DISTINCT ?command ?type ?location
                 |    WHERE {
                 |      GRAPH ${GraphClass.Project.id(projectId)} {
                 |        $activityId prov:qualifiedAssociation/prov:hadPlan ?planId;
                 |                                      a ?type.
                 |        OPTIONAL { ?planId renku:command ?maybeCommand. }
                 |      }
                 |      BIND (IF(BOUND(?maybeCommand), CONCAT(STR(?maybeCommand), STR(' ')), '') AS ?command).
                 |      BIND ($activityId AS ?location)
                 |    }
                 |  } {
                 |    SELECT ?commandParameter
                 |    WHERE {
                 |      { # inputs
                 |        GRAPH ${GraphClass.Project.id(projectId)} {
                 |          $activityId prov:qualifiedAssociation/prov:hadPlan ?planId.
                 |          ?planId renku:hasInputs ?input .
                 |          ?paramValue a renku:ParameterValue ;
                 |                      schema:valueReference ?input .
                 |          ?paramValue schema:value ?value .
                 |          OPTIONAL { ?input renku:mappedTo/renku:streamType ?maybeStreamType }
                 |          OPTIONAL { ?input renku:position ?maybePosition }
                 |          OPTIONAL { ?input renku:prefix ?maybePrefix }
                 |        }
                 |        BIND (IF(BOUND(?maybeStreamType), ?maybeStreamType, '') AS ?streamType).
                 |        BIND (IF(?streamType = 'stdin', '< ', '') AS ?streamOperator).
                 |        BIND (IF(BOUND(?maybePrefix), STR(?maybePrefix), '') AS ?prefix).
                 |        BIND (IF(BOUND(?maybePosition), CONCAT(?prefix, ?streamOperator, STR(?value)), '') AS ?commandParameter) .
                 |      } UNION { # outputs
                 |        GRAPH ${GraphClass.Project.id(projectId)} {
                 |          $activityId prov:qualifiedAssociation/prov:hadPlan ?planId.
                 |          ?planId renku:hasOutputs ?output .
                 |          ?paramValue a renku:ParameterValue ;
                 |                      schema:valueReference ?output .
                 |          ?paramValue schema:value ?value .
                 |          OPTIONAL { ?output renku:position ?maybePosition }
                 |          OPTIONAL { ?output renku:mappedTo/renku:streamType ?maybeStreamType }
                 |          OPTIONAL { ?output renku:prefix ?maybePrefix }
                 |        }
                 |        BIND (IF(BOUND(?maybeStreamType), ?maybeStreamType, '') AS ?streamType).
                 |        BIND (IF(?streamType = 'stdout', '> ', IF(?streamType = 'stderr', '2> ', '')) AS ?streamOperator).
                 |        BIND (IF(BOUND(?maybePrefix), STR(?maybePrefix), '') AS ?prefix) .
                 |        BIND (IF(BOUND(?maybePosition), CONCAT(?prefix, ?streamOperator, STR(?value)), '') AS ?commandParameter) .
                 |      } UNION { # parameters
                 |        GRAPH ${GraphClass.Project.id(projectId)} {
                 |          $activityId prov:qualifiedAssociation/prov:hadPlan ?planId.
                 |          ?planId renku:hasArguments ?parameter .
                 |          ?paramValue a renku:ParameterValue;
                 |                      schema:valueReference ?parameter .
                 |          ?paramValue schema:value ?value .
                 |          OPTIONAL { ?parameter renku:position ?maybePosition }
                 |          OPTIONAL { ?parameter renku:prefix ?maybePrefix }
                 |        }
                 |        BIND (IF(BOUND(?maybePrefix), STR(?maybePrefix), '') AS ?prefix) .
                 |        BIND (IF(BOUND(?maybePosition), CONCAT(?prefix, STR(?value)), '') AS ?commandParameter) .
                 |      }
                 |    }
                 |    GROUP BY ?maybePosition ?commandParameter
                 |    HAVING (COUNT(*) > 0)
                 |    ORDER BY ?maybePosition
                 |  }
                 |}
                 |GROUP BY ?command ?type ?location
                 |""".stripMargin
      )
  }
}
