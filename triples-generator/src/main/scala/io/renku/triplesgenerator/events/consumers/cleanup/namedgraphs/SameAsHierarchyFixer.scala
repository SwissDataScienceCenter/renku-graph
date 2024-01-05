/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.MonadThrow
import cats.data.Nested
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.datasets._
import io.renku.graph.model.{GraphClass, datasets, projects}
import io.renku.jsonld.{EntityId, NamedGraph}
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private object SameAsHierarchyFixer {
  def relinkSameAsHierarchy[F[_]: Async: Logger: SparqlQueryTimeRecorder](slug: projects.Slug)(implicit
      connectionConfig: ProjectsConnectionConfig
  ): F[Unit] = MonadThrow[F].catchNonFatal(new SameAsHierarchyFixer[F](slug)(connectionConfig)) >>= (_.run())
}

private class SameAsHierarchyFixer[F[_]: Async: Logger: SparqlQueryTimeRecorder](slug: projects.Slug)(
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig) {

  import io.renku.jsonld.syntax._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  def run(): F[Unit] = (collectDSInfos map relinkIfNeeded).sequence

  private type DSInfo = (EntityId, datasets.ResourceId, TopmostSameAs, Option[SameAs])

  private def relinkIfNeeded(dsInfo: DSInfo): F[Unit] = {
    val (graphId, dsId, topmostSameAs, _) = dsInfo

    collectDSsProjects(graphId) >>= {
      case _ :: Nil if dsId.show == topmostSameAs.show =>
        nominateNewTopAndUpdateDescendants(dsInfo)
      case _ :: Nil =>
        fixSameAsOnDirectDescendants(dsInfo) >> fixTopmostSameAsOnAllDescendants(dsInfo)
      case _ =>
        ().pure[F]
    }
  }

  private def collectDSsProjects(graphId: EntityId): F[List[projects.ResourceId]] = {
    implicit val decoder: Decoder[List[projects.ResourceId]] = ResultsDecoder[List, projects.ResourceId] {
      implicit cursor => extract[projects.ResourceId]("allProjectIds")
    }

    queryExpecting[List[projects.ResourceId]] {
      SparqlQuery.of(
        name = "find DS projects",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT DISTINCT ?allProjectIds
                 |WHERE {
                 |  GRAPH $graphId {
                 |    ?datasetId ^renku:hasDataset $graphId
                 |  }
                 |  GRAPH ?projectGraphs {
                 |    ?datasetId ^renku:hasDataset ?allProjectIds
                 |  }
                 |}""".stripMargin
      )
    }
  }

  private def collectDSInfos: Nested[F, List, DSInfo] = Nested {
    implicit val decoder: Decoder[List[DSInfo]] = ResultsDecoder[List, DSInfo] { implicit cursor =>
      (extract[projects.ResourceId]("projectId").map(GraphClass.Project.id),
       extract[datasets.ResourceId]("datasetId"),
       extract[TopmostSameAs]("topmostSameAs"),
       extract[Option[SameAs]]("sameAs")
      ).mapN((_, _, _, _))
    }

    queryExpecting[List[DSInfo]] {
      SparqlQuery.of(
        name = "find proj DS topmostSameAs",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?projectId ?datasetId ?topmostSameAs ?sameAs
                 |WHERE {
                 |  GRAPH ?g {
                 |    ?projectId renku:projectPath ${slug.asObject}.
                 |    ?datasetId ^renku:hasDataset ?projectId;
                 |               a schema:Dataset;
                 |               renku:topmostSameAs ?topmostSameAs.
                 |    OPTIONAL { ?datasetId schema:sameAs/schema:url ?sameAs }
                 |  }
                 |}""".stripMargin
      )
    }
  }

  private case class DirectDescendantInfo(graphId:       EntityId,
                                          dsId:          datasets.ResourceId,
                                          topmostSameAs: TopmostSameAs,
                                          sameAs:        SameAs,
                                          modified:      Boolean
  )

  private def collectDirectDescendants(dsInfo: DSInfo): Nested[F, List, DirectDescendantInfo] = Nested {
    val (_, dsId, _, _) = dsInfo

    implicit val decoder: Decoder[List[DirectDescendantInfo]] = ResultsDecoder[List, DirectDescendantInfo] {
      implicit cursor =>
        for {
          graphId             <- extract[projects.ResourceId]("graphId").map(GraphClass.Project.id)
          descId              <- extract[datasets.ResourceId]("descendantId")
          descTopmostSameAs   <- extract[TopmostSameAs]("descendantTopmostSameAs")
          sameAs              <- extract[SameAs]("descendantSameAs")
          maybeModificationId <- extract[Option[ResourceId]]("modificationId")
        } yield DirectDescendantInfo(graphId,
                                     descId,
                                     descTopmostSameAs,
                                     sameAs,
                                     modified = maybeModificationId.nonEmpty
        )
    }

    queryExpecting[List[DirectDescendantInfo]] {
      SparqlQuery.of(
        name = "find DS descendants info",
        Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?graphId ?descendantId ?descendantTopmostSameAs ?descendantSameAs ?modificationId
                 |WHERE {
                 |  GRAPH ?graphId {
                 |    ?descendantId schema:sameAs/schema:url ${dsId.asEntityId};
                 |                  renku:topmostSameAs ?descendantTopmostSameAs;
                 |                  schema:sameAs/schema:url ?descendantSameAs.
                 |    OPTIONAL { ?modificationId prov:wasDerivedFrom/schema:url ?descendantId }
                 |  }
                 |}""".stripMargin
      )
    }
  }

  private def nominateNewTopAndUpdateDescendants(dsInfo: DSInfo): F[Unit] =
    collectDirectDescendants(dsInfo).value >>= { directDescendants =>
      chooseDescendantAsReplacement(directDescendants) match {
        case None => ().pure[F]
        case Some(nominatedDescendant) =>
          makeDescendantTheNewTop(nominatedDescendant) >>= { newTopInfo =>
            val directDescendantsUpdate = directDescendants
              .filterNot(_.dsId == newTopInfo.dsId)
              .map(updateDirectDescendants(_, newTopInfo))
              .sequence
              .void

            directDescendantsUpdate >> updateNonDirectDescendants(dsInfo, newTopInfo)
          }
      }
    }

  private def chooseDescendantAsReplacement(descendants: List[DirectDescendantInfo]): Option[DirectDescendantInfo] =
    descendants.find(!_.modified) match {
      case nonModified @ Some(_) => nonModified
      case _                     => descendants.headOption
    }

  private def makeDescendantTheNewTop(nominatedDescendant: DirectDescendantInfo): F[DirectDescendantInfo] =
    cleanUpDescendantSameAs(nominatedDescendant) >>
      cleanUpDescendantTopmostSameAs(nominatedDescendant) >>
      linkDescendantToItself(nominatedDescendant) >>
      insertNewSameAs(nominatedDescendant.graphId, nominatedDescendant) map (formNewTopInfo(nominatedDescendant, _))

  private def cleanUpDescendantSameAs(descendant: DirectDescendantInfo) = updateWithNoResult {
    SparqlQuery.of(
      name = "clean-up DS descendant SameAs",
      Prefixes of schema -> "schema",
      sparql"""|DELETE {
               |  GRAPH ${descendant.graphId} {
               |    ?descendantSameAs ?p ?s.
               |    ${descendant.dsId.asEntityId} schema:sameAs ?descendantSameAs
               |  }
               |}
               |WHERE {
               |  GRAPH ${descendant.graphId} {
               |    ${descendant.dsId.asEntityId} schema:sameAs ?descendantSameAs.
               |    ?descendantSameAs ?p ?s
               |  }
               |}""".stripMargin
    )
  }

  private def cleanUpDescendantTopmostSameAs(descendant: DirectDescendantInfo) = updateWithNoResult {
    val DirectDescendantInfo(graphId, descendantId, topmostSameAs, _, _) = descendant

    SparqlQuery.of(
      name = "clean-up DS descendant TopmostSameAs",
      Prefixes of renku -> "renku",
      sparql"""|DELETE DATA {
               |  GRAPH $graphId {
               |    ${descendantId.asEntityId} renku:topmostSameAs ${topmostSameAs.asEntityId}
               |  }
               |}""".stripMargin
    )
  }

  private def linkDescendantToItself(nominated: DirectDescendantInfo) = updateWithNoResult {
    SparqlQuery.of(
      name = "link DS to itself",
      Prefixes of renku -> "renku",
      sparql"""|INSERT DATA {
               |  GRAPH ${nominated.graphId} {
               |    ${nominated.dsId.asEntityId} renku:topmostSameAs ${nominated.dsId.asEntityId}
               |  }
               |}""".stripMargin
    )
  }

  private def insertNewSameAs(graphId: EntityId, newTop: DirectDescendantInfo): F[InternalSameAs] = {
    val newTopSameAs = InternalSameAs(newTop.dsId.show)
    insertNewSameAs(graphId, newTopSameAs).map(_ => newTopSameAs)
  }

  private def insertNewSameAs(graphId: EntityId, sameAs: SameAs): F[Unit] =
    upload(NamedGraph.fromJsonLDsUnsafe(graphId, sameAs.asJsonLD))

  private def formNewTopInfo(newTopInfo: DirectDescendantInfo, sameAs: SameAs) =
    newTopInfo.copy(topmostSameAs = TopmostSameAs(sameAs), sameAs = sameAs)

  private def updateDirectDescendants(descendant: DirectDescendantInfo, newTop: DirectDescendantInfo): F[Unit] =
    cleanUpDescendantSameAs(descendant) >>
      cleanUpDescendantTopmostSameAs(descendant) >>
      insertNewSameAs(descendant.graphId, newTop.sameAs) >>
      linkDescendant(descendant, newTop)

  private def linkDescendant(descendantInfo: DirectDescendantInfo, newTop: DirectDescendantInfo) = updateWithNoResult {
    val DirectDescendantInfo(_, _, newTopmostSameAs, newSameAs, _) = newTop
    SparqlQuery.of(
      name = "link DS to new top",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|INSERT DATA {
               |  GRAPH ${descendantInfo.graphId} {
               |    ${descendantInfo.dsId.asEntityId} renku:topmostSameAs ${newTopmostSameAs.asEntityId}.
               |    ${descendantInfo.dsId.asEntityId} schema:sameAs ${newSameAs.asEntityId}
               |  }
               |}""".stripMargin
    )
  }

  private def updateNonDirectDescendants(dsInfo: DSInfo, newTop: DirectDescendantInfo): F[Unit] =
    collectDescendantsThroughTopmost(dsInfo)
      .map(descendant => cleanUpTopmostSameAs(descendant) >> insertNewTopmostSameAs(descendant, newTop))
      .sequence

  private type DescendantInfo = (EntityId, ResourceId, SameAs)

  private def collectDescendantsThroughTopmost(dsInfo: DSInfo): Nested[F, List, DescendantInfo] = Nested {
    implicit val decoder: Decoder[List[DescendantInfo]] = ResultsDecoder[List, DescendantInfo](implicit cursor =>
      (extract[projects.ResourceId]("graphId").map(GraphClass.Project.id),
       extract[datasets.ResourceId]("descendantId"),
       extract[SameAs]("descendantSameAs")
      ).mapN((_, _, _))
    )

    val (_, dsId, _, _) = dsInfo

    queryExpecting[List[DescendantInfo]] {
      SparqlQuery.of(
        name = "find DS descendants info",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?graphId ?descendantId ?descendantSameAs
                 |WHERE {
                 |  GRAPH ?graphId {
                 |    ?descendantId renku:topmostSameAs ${dsId.asEntityId};
                 |                  schema:sameAs/schema:url ?descendantSameAs
                 |  }
                 |}""".stripMargin
      )
    }
  }

  private def cleanUpTopmostSameAs(descendantInfo: DescendantInfo) = updateWithNoResult {
    val (graphId, descendantId, _) = descendantInfo

    SparqlQuery.of(
      name = "clean-up DS descendant",
      Prefixes of renku -> "renku",
      sparql"""|DELETE {
               |  GRAPH $graphId {
               |    ${descendantId.asEntityId} renku:topmostSameAs ?topmost
               |  }
               |}
               |WHERE {
               |  GRAPH $graphId {
               |    ${descendantId.asEntityId} renku:topmostSameAs ?topmost
               |  }
               |}""".stripMargin
    )
  }

  private def insertNewTopmostSameAs(descendantInfo: DescendantInfo, newTop: DirectDescendantInfo): F[Unit] =
    insertNewTopmostSameAs(descendantInfo, newTop.topmostSameAs)

  private def insertNewTopmostSameAs(descendantInfo: DescendantInfo, newTopmostSameAs: TopmostSameAs): F[Unit] =
    updateWithNoResult {
      val (graphId, descendantId, _) = descendantInfo

      SparqlQuery.of(
        name = "insert new topmostSameAs",
        Prefixes of renku -> "renku",
        sparql"""|INSERT DATA {
                 |  GRAPH $graphId {
                 |    ${descendantId.asEntityId} renku:topmostSameAs ${newTopmostSameAs.asEntityId}
                 |  }
                 |}""".stripMargin
      )
    }

  private def fixSameAsOnDirectDescendants(dsInfo: DSInfo) =
    (collectDirectDescendants(dsInfo) map (relinkSameAs(dsInfo, _))).sequence

  private def relinkSameAs(dsInfo: DSInfo, descendantInfo: DirectDescendantInfo): F[Unit] =
    cleanUpDescendantSameAs(descendantInfo) >> {
      val (_, _, _, maybeSameAs) = dsInfo
      maybeSameAs match {
        case None => ().pure[F]
        case Some(sameAs) =>
          insertNewSameAs(descendantInfo.graphId, sameAs) >> linkDescendantToSameAs(descendantInfo, sameAs)
      }
    }

  private def linkDescendantToSameAs(descendantInfo: DirectDescendantInfo, sameAs: SameAs) =
    updateWithNoResult {
      SparqlQuery.of(
        name = "insert new SameAs",
        Prefixes of schema -> "schema",
        sparql"""|INSERT DATA {
                 |  GRAPH ${descendantInfo.graphId} {
                 |    ${descendantInfo.dsId.asEntityId} schema:sameAs ${sameAs.asEntityId}
                 |  }
                 |}""".stripMargin
      )
    }

  private def fixTopmostSameAsOnAllDescendants(dsInfo: DSInfo) =
    collectDescendantsThroughTopmost(dsInfo).map { descendant =>
      for {
        _               <- cleanUpTopmostSameAs(descendant)
        maybeNewTopmost <- findTopmostSameAsOnDsWith(descendant)
        _               <- maybeNewTopmost.fold(ifEmpty = ().pure[F])(insertNewTopmostSameAs(descendant, _))
      } yield ()
    }.sequence

  private def findTopmostSameAsOnDsWith(descendant: DescendantInfo): F[Option[TopmostSameAs]] = {
    val (graphId, _, sameAs) = descendant
    val decoder: Decoder[Option[TopmostSameAs]] =
      ResultsDecoder[Option, TopmostSameAs](implicit cursor => extract[TopmostSameAs]("topmostSameAs"))

    queryExpecting[Option[TopmostSameAs]] {
      SparqlQuery.of(
        name = "find TopmostSameAs by SameAs",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?topmostSameAs
                 |WHERE {
                 |  GRAPH $graphId {
                 |    ?dsId schema:sameAs ${sameAs.asEntityId};
                 |          renku:topmostSameAs ?topmostSameAs
                 |  }
                 |}
                 |LIMIT 1""".stripMargin
      )
    }(decoder)
  }

  private implicit class NestedOps[O](nested: Nested[F, List, F[O]]) {
    lazy val sequence: F[Unit] = nested.value.flatMap(_.sequence).void
  }
}
