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

package io.renku.knowledgegraph.lineage

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.Schemas._
import io.renku.graph.model.projects.ResourceId
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.knowledgegraph.lineage.model.{ExecutionInfo, Node}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import io.renku.tinytypes.json.TinyTypeDecoders
import org.typelevel.log4cats.Logger

private trait NodeDetailsFinder[F[_]] {

  def findDetails[T](
      location:     Set[T],
      projectPath:  projects.Path
  )(implicit query: (T, ResourceId) => SparqlQuery): F[Set[Node]]
}

private class NodeDetailsFinderImpl[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig,
    renkuUrl:       RenkuUrl
) extends RdfStoreClientImpl[F](rdfStoreConfig)
    with NodeDetailsFinder[F] {

  override def findDetails[T](
      ids:          Set[T],
      projectPath:  projects.Path
  )(implicit query: (T, ResourceId) => SparqlQuery): F[Set[Node]] =
    ids.toList
      .map { id =>
        queryExpecting[Option[Node]](using = query(id, ResourceId(projectPath)(renkuUrl)))
          .flatMap(failIf(no = id))
      }
      .parSequence
      .map(_.toSet)

  private implicit val nodeDecoder: Decoder[Option[Node]] = {
    implicit val locationDecoder: Decoder[Node.Location] = TinyTypeDecoders.stringDecoder(Node.Location)
    implicit val labelDecoder:    Decoder[Node.Label]    = TinyTypeDecoders.stringDecoder(Node.Label)
    implicit val typeDecoder:     Decoder[Node.Type]     = TinyTypeDecoders.stringDecoder(Node.Type)

    implicit lazy val fieldsDecoder: Decoder[Option[(Node.Location, Node.Type, Node.Label)]] = { cursor =>
      for {
        maybeNodeType <- cursor.downField("type").downField("value").as[Option[Node.Type]]
        maybeLocation <- cursor.downField("location").downField("value").as[Option[Node.Location]]
        maybeLabel    <- cursor.downField("label").downField("value").as[Option[Node.Label]].map(trimValue)
      } yield (maybeLocation, maybeNodeType, maybeLabel) mapN ((_, _, _))
    }

    lazy val trimValue: Option[Node.Label] => Option[Node.Label] = _.map(l => Node.Label(l.value.trim))

    lazy val maybeToNode: List[(Node.Location, Node.Type, Node.Label)] => Option[Node] = {
      case Nil => None
      case (location, typ, label) :: tail =>
        Some {
          tail.foldLeft(Node(location, label, Set(typ))) {
            case (node, (`location`, t, `label`)) => node.copy(types = node.types + t)
            case (node, _)                        => node
          }
        }
    }

    _.downField("results")
      .downField("bindings")
      .as[List[Option[(Node.Location, Node.Type, Node.Label)]]]
      .map(_.flatten)
      .map(maybeToNode)
  }

  private def failIf[T](no: T): Option[Node] => F[Node] = {
    case Some(details) => details.pure[F]
    case _ =>
      no match {
        case location: Node.Location =>
          new IllegalArgumentException(s"No entity with $location").raiseError[F, Node]
        case runInfo: ExecutionInfo =>
          new IllegalArgumentException(s"No plan with ${runInfo.entityId}").raiseError[F, Node]
        case other =>
          new IllegalArgumentException(s"Entity $other not recognisable").raiseError[F, Node]
      }
  }
}

private object NodeDetailsFinder {

  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder]: F[NodeDetailsFinder[F]] = for {
    config   <- RdfStoreConfig[F]()
    renkuUrl <- RenkuUrlLoader[F]()
  } yield new NodeDetailsFinderImpl[F](config, renkuUrl)

  implicit val locationQuery: (Node.Location, ResourceId) => SparqlQuery = (location, path) =>
    SparqlQuery.of(
      name = "lineage - entity details",
      Prefixes.of(prov -> "prov", schema -> "schema", renku -> "renku"),
      s"""|SELECT DISTINCT ?type ?location ?label
          |WHERE {
          |  { 
          |    ?entity prov:atLocation '$location';
          |          a prov:Entity;
          |          a ?type .
          |  } {
          |    ?activityId a prov:Activity ;
          |                prov:qualifiedUsage / prov:entity ?entity ;
          |                ^renku:hasActivity ${path.showAs[RdfResource]} .
          |  } UNION {
          |    ?entity prov:qualifiedGeneration / prov:activity / ^renku:hasActivity ${path.showAs[RdfResource]} .
          |  }
          |  BIND ('$location' AS ?location)
          |  BIND ('$location' AS ?label)
          |}
          |""".stripMargin
    )

  implicit val activityIdQuery: (ExecutionInfo, ResourceId) => SparqlQuery = { case (ExecutionInfo(activityId, _), _) =>
    SparqlQuery.of(
      name = "lineage - plan details",
      Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
      s"""|SELECT DISTINCT ?type (CONCAT(STR(?command), (GROUP_CONCAT(?commandParameter; separator=' '))) AS ?label) ?location
          |WHERE {
          |  {
          |    SELECT DISTINCT ?command ?type ?location
          |    WHERE {
          |      <$activityId> prov:qualifiedAssociation/prov:hadPlan ?planId;
          |                    a ?type.
          |      OPTIONAL { ?planId renku:command ?maybeCommand. }
          |      BIND (IF(bound(?maybeCommand), CONCAT(STR(?maybeCommand), STR(' ')), '') AS ?command).
          |      BIND (<$activityId> AS ?location)
          |    }
          |  } {
          |    SELECT ?position ?commandParameter
          |     WHERE {
          |      { # inputs
          |        <$activityId> prov:qualifiedAssociation/prov:hadPlan ?planId.
          |        ?planId renku:hasInputs ?input .
          |        ?paramValue a renku:ParameterValue ;
          |                    schema:valueReference ?input .
          |        ?paramValue schema:value ?value .
          |        OPTIONAL { ?input renku:mappedTo/renku:streamType ?maybeStreamType. }
          |        ?input renku:position ?position.
          |        OPTIONAL { ?input renku:prefix ?maybePrefix }
          |        BIND (IF(bound(?maybeStreamType), ?maybeStreamType, '') AS ?streamType).
          |        BIND (IF(?streamType = 'stdin', '< ', '') AS ?streamOperator).
          |        BIND (IF(bound(?maybePrefix), STR(?maybePrefix), '') AS ?prefix).
          |        BIND (CONCAT(?prefix, ?streamOperator, STR(?value)) AS ?commandParameter) .
          |      } UNION { # outputs
          |        <$activityId> prov:qualifiedAssociation/prov:hadPlan ?planId.
          |        ?planId renku:hasOutputs ?output .
          |        ?paramValue a renku:ParameterValue ;
          |                    schema:valueReference ?output .
          |        ?paramValue schema:value ?value .
          |        ?output renku:position ?position.
          |        OPTIONAL { ?output renku:mappedTo/renku:streamType ?maybeStreamType. }
          |        OPTIONAL { ?output renku:prefix ?maybePrefix }
          |        BIND (IF(bound(?maybeStreamType), ?maybeStreamType, '') AS ?streamType).
          |        BIND (IF(?streamType = 'stdout', '> ', IF(?streamType = 'stderr', '2> ', '')) AS ?streamOperator).
          |        BIND (IF(bound(?maybePrefix), STR(?maybePrefix), '') AS ?prefix) .
          |        BIND (CONCAT(?prefix, ?streamOperator, STR(?value)) AS ?commandParameter) .
          |      } UNION { # parameters
          |        <$activityId> prov:qualifiedAssociation/prov:hadPlan ?planId.
          |        ?planId renku:hasArguments ?parameter .
          |        ?paramValue a renku:ParameterValue;
          |                    schema:valueReference ?parameter .
          |        ?paramValue schema:value ?value .
          |        ?parameter renku:position ?position .
          |        OPTIONAL { ?parameter renku:prefix ?maybePrefix }
          |        BIND (IF(bound(?maybePrefix), STR(?maybePrefix), '') AS ?prefix) .
          |        BIND (CONCAT(?prefix, STR(?value)) AS ?commandParameter) .
          |      }
          |    }
          |    GROUP BY ?position ?commandParameter
          |    HAVING (COUNT(*) > 0)
          |    ORDER BY ?position
          |  }
          |}
          |GROUP BY ?command ?type ?location
          |""".stripMargin
    )
  }
}
