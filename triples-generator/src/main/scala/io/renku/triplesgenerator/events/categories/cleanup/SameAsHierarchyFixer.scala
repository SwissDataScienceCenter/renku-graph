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

package io.renku.triplesgenerator.events.categories.cleanup

import cats.MonadThrow
import cats.data.Nested
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.datasets._
import io.renku.graph.model.projects
import io.renku.jsonld.EntityId
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private object SameAsHierarchyFixer {
  def relinkSameAsHierarchy[F[_]: Async: Logger: SparqlQueryTimeRecorder](path: projects.Path)(implicit
      rdfStoreConfig: RdfStoreConfig
  ): F[Unit] = MonadThrow[F].catchNonFatal {
    new SameAsHierarchyFixer[F](path)(rdfStoreConfig)
  } >>= (_.run())
}

private class SameAsHierarchyFixer[F[_]: Async: Logger: SparqlQueryTimeRecorder](path: projects.Path)(
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig,
                             idleTimeoutOverride = (10 minutes).some,
                             requestTimeoutOverride = (10 minutes).some
    ) {

  import io.renku.jsonld.syntax._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  def run(): F[Unit] = (collectDSInfos map relinkIfNeeded).sequence

  private type DSInfo = (ResourceId, TopmostSameAs, Option[SameAs])

  private def relinkIfNeeded(dsInfo: DSInfo): F[Unit] = {
    val (dsId, topmostSameAs, _) = dsInfo

    collectDSsProjects >>= {
      case _ :: Nil if dsId.show == topmostSameAs.show =>
        nominateNewTopAndUpdateDescendants(dsInfo)
      case _ :: Nil =>
        fixSameAsOnDirectDescendants(dsInfo) >> fixTopmostSameAsOnAllDescendants(dsInfo)
      case _ =>
        ().pure[F]
    }
  }

  private def collectDSsProjects: F[List[projects.Path]] = {
    implicit val decoder: Decoder[List[projects.Path]] = ResultsDecoder[projects.Path] { implicit cursor =>
      extract[projects.Path]("path")
    }

    queryExpecting[List[projects.Path]] {
      SparqlQuery.of(
        name = "find DS projects",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""
        SELECT DISTINCT ?path
        WHERE {
          ?projectId renku:projectPath '$path'.
          ?datasetId ^renku:hasDataset ?projectId.
          ?datasetId ^renku:hasDataset ?allProjectIds.
          ?allProjectIds renku:projectPath ?path.
        }
        """
      )
    }
  }

  private def collectDSInfos: Nested[F, List, DSInfo] = Nested {
    implicit val decoder: Decoder[List[DSInfo]] = ResultsDecoder[DSInfo] { implicit cursor =>
      for {
        id            <- extract[ResourceId]("datasetId")
        topmostSameAs <- extract[TopmostSameAs]("topmostSameAs")
        maybeSameAs   <- extract[Option[SameAs]]("sameAs")
      } yield (id, topmostSameAs, maybeSameAs)
    }

    queryExpecting[List[DSInfo]] {
      SparqlQuery.of(
        name = "find proj DS topmostSameAs",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""
        SELECT ?datasetId ?topmostSameAs ?sameAs
        WHERE {
          ?projectId renku:projectPath '$path'.
          ?datasetId ^renku:hasDataset ?projectId;
                     a schema:Dataset;
                     renku:topmostSameAs ?topmostSameAs.
          OPTIONAL { ?datasetId schema:sameAs ?sameAs }
        }
        """
      )
    }
  }

  private case class DirectDescendantInfo(id:            ResourceId,
                                          topmostSameAs: TopmostSameAs,
                                          sameAs:        SameAs,
                                          date:          Date,
                                          modified:      Boolean
  )

  private def collectDirectDescendants(dsInfo: DSInfo): Nested[F, List, DirectDescendantInfo] = Nested {
    val (dsId, _, _) = dsInfo

    implicit val decoder: Decoder[List[DirectDescendantInfo]] =
      ResultsDecoder[DirectDescendantInfo] { implicit cursor =>
        for {
          id                  <- extract[ResourceId]("descendantId")
          topmostSameAs       <- extract[TopmostSameAs]("descendantTopmostSameAs")
          sameAs              <- extract[SameAs]("descendantSameAs")
          maybeDatePublished  <- extract[Option[DatePublished]]("datePublished")
          maybeDateCreated    <- extract[Option[DateCreated]]("dateCreated")
          maybeModificationId <- extract[Option[ResourceId]]("modificationId")
          date <- maybeDatePublished
                    .orElse(maybeDateCreated)
                    .map(_.asRight)
                    .getOrElse(DecodingFailure(s"No dates on DS $dsId", Nil).asLeft)
        } yield DirectDescendantInfo(id, topmostSameAs, sameAs, date, modified = maybeModificationId.nonEmpty)
      }

    queryExpecting[List[DirectDescendantInfo]] {
      SparqlQuery.of(
        name = "find DS descendants info",
        Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
        s"""
        SELECT ?descendantId ?descendantTopmostSameAs ?descendantSameAs 
          ?datePublished ?dateCreated ?modificationId
        WHERE {
          ?descendantId schema:sameAs/schema:url <${dsId.show}>;
                        renku:topmostSameAs ?descendantTopmostSameAs;
                        schema:sameAs ?descendantSameAs;
          OPTIONAL { ?descendantId schema:datePublished ?datePublished }
          OPTIONAL { ?descendantId schema:dateCreated ?dateCreated }
          OPTIONAL { ?modificationId prov:wasDerivedFrom/schema:url ?descendantId }
        }
        """
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
              .filterNot(_.id == newTopInfo.id)
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

  private def makeDescendantTheNewTop(descendantInfo: DirectDescendantInfo): F[DirectDescendantInfo] =
    cleanUpDescendantSameAs(descendantInfo) >>
      cleanUpDescendantTopmostSameAs(descendantInfo) >>
      linkDescendantToItself(descendantInfo) >>
      insertNewSameAs(descendantInfo) map (formNewTopInfo(descendantInfo, _))

  private def cleanUpDescendantSameAs(descendantInfo: DirectDescendantInfo) = updateWithNoResult {
    SparqlQuery.of(
      name = "clean-up DS descendant SameAs",
      Prefixes of schema -> "schema",
      s"""
      DELETE {
        ?descendantSameAs ?p ?s.
        <${descendantInfo.id.show}> schema:sameAs ?descendantSameAs.
      }
      WHERE {
        <${descendantInfo.id.show}> schema:sameAs ?descendantSameAs.
        ?descendantSameAs ?p ?s.
      }
      """
    )
  }

  private def cleanUpDescendantTopmostSameAs(descendantInfo: DirectDescendantInfo) = updateWithNoResult {
    val DirectDescendantInfo(descendantId, topmostSameAs, _, _, _) = descendantInfo

    SparqlQuery.of(
      name = "clean-up DS descendant TopmostSameAs",
      Prefixes of renku -> "renku",
      s"""
      DELETE DATA {
        <${descendantId.show}> renku:topmostSameAs <${topmostSameAs.show}>.
      }
      """
    )
  }

  private def linkDescendantToItself(descendantInfo: DirectDescendantInfo) = updateWithNoResult {
    SparqlQuery.of(
      name = "link DS to itself",
      Prefixes of renku -> "renku",
      s"""
      INSERT DATA {
        <${descendantInfo.id.show}> renku:topmostSameAs <${descendantInfo.id.show}>
      }
      """
    )
  }

  private def insertNewSameAs(newTop: DirectDescendantInfo): F[InternalSameAs] = {
    val newTopSameAs = InternalSameAs(newTop.id.show)
    upload(newTopSameAs.asJsonLD).map(_ => newTopSameAs)
  }

  private def formNewTopInfo(newTopInfo: DirectDescendantInfo, sameAs: SameAs) =
    newTopInfo.copy(topmostSameAs = TopmostSameAs(sameAs), sameAs = sameAs)

  private def updateDirectDescendants(descendantInfo: DirectDescendantInfo, newTop: DirectDescendantInfo): F[Unit] =
    cleanUpDescendantSameAs(descendantInfo) >>
      cleanUpDescendantTopmostSameAs(descendantInfo) >>
      linkDescendant(descendantInfo, newTop)

  private def linkDescendant(descendantInfo: DirectDescendantInfo, newTop: DirectDescendantInfo) = {
    val DirectDescendantInfo(_, newTopmostSameAs, newSameAs, _, _) = newTop

    newSameAs.asJsonLD.entityId
      .map(_.pure[F])
      .getOrElse(new Exception(show"Cannot obtain EntityId for $newSameAs").raiseError[F, EntityId]) >>= {
      newSameAsId =>
        updateWithNoResult {
          SparqlQuery.of(
            name = "link DS to new top",
            Prefixes of (renku -> "renku", schema -> "schema"),
            s"""
            INSERT DATA {
              <${descendantInfo.id.show}> renku:topmostSameAs <${newTopmostSameAs.show}>.
              <${descendantInfo.id.show}> schema:sameAs <${newSameAsId.show}>.
            }
            """
          )
        }
    }
  }

  private def updateNonDirectDescendants(dsInfo: DSInfo, newTop: DirectDescendantInfo): F[Unit] =
    collectDescendantsThroughTopmost(dsInfo)
      .map(descendant => cleanUpTopmostSameAs(descendant) >> insertNewTopmostSameAs(descendant, newTop))
      .sequence

  private type DescendantInfo = (ResourceId, SameAs)

  private def collectDescendantsThroughTopmost(dsInfo: DSInfo): Nested[F, List, DescendantInfo] = Nested {
    implicit val decoder: Decoder[List[DescendantInfo]] =
      ResultsDecoder[DescendantInfo](implicit cursor =>
        (extract[ResourceId]("descendantId") -> extract[SameAs]("descendantSameAs")).mapN(_ -> _)
      )

    val (dsId, _, _) = dsInfo

    queryExpecting[List[DescendantInfo]] {
      SparqlQuery.of(
        name = "find DS descendants info",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""
        SELECT ?descendantId ?descendantSameAs
        WHERE {
          ?descendantId renku:topmostSameAs <${dsId.show}>;
                        schema:sameAs ?descendantSameAs.
        }
        """
      )
    }
  }

  private def cleanUpTopmostSameAs(descendantInfo: DescendantInfo) = updateWithNoResult {
    val (descendantId, _) = descendantInfo

    SparqlQuery.of(
      name = "clean-up DS descendant",
      Prefixes of renku -> "renku",
      s"""
      DELETE {
        <${descendantId.show}> renku:topmostSameAs ?topmost.
      }
      WHERE {
        <${descendantId.show}> renku:topmostSameAs ?topmost
      }
      """
    )
  }

  private def insertNewTopmostSameAs(descendantInfo: DescendantInfo, newTop: DirectDescendantInfo): F[Unit] =
    insertNewTopmostSameAs(descendantInfo, newTop.topmostSameAs)

  private def insertNewTopmostSameAs(descendantInfo: DescendantInfo, newTopmostSameAs: TopmostSameAs): F[Unit] =
    updateWithNoResult {
      val (descendantId, _) = descendantInfo

      SparqlQuery.of(
        name = "insert new topmostSameAs",
        Prefixes of renku -> "renku",
        s"""
        INSERT DATA {
          <${descendantId.show}> renku:topmostSameAs <${newTopmostSameAs.show}> .
        }
        """
      )
    }

  private def fixSameAsOnDirectDescendants(dsInfo: DSInfo) =
    (collectDirectDescendants(dsInfo) map (relinkSameAs(dsInfo, _))).sequence

  private def relinkSameAs(dsInfo: DSInfo, descendantInfo: DirectDescendantInfo): F[Unit] =
    cleanUpDescendantSameAs(descendantInfo) >> linkDescendantToSameAsIfExists(descendantInfo, dsInfo)

  private def linkDescendantToSameAsIfExists(descendantInfo: DirectDescendantInfo, dsInfo: DSInfo) = {
    val (_, _, maybeSameAs) = dsInfo
    maybeSameAs match {
      case None => ().pure[F]
      case Some(sameAs) =>
        updateWithNoResult {
          SparqlQuery.of(
            name = "insert new SameAs",
            Prefixes of schema -> "schema",
            s"""
            INSERT DATA {
              <${descendantInfo.id.show}> schema:sameAs <${sameAs.show}>
            }
            """
          )
        }
    }
  }

  private def fixTopmostSameAsOnAllDescendants(dsInfo: DSInfo) =
    collectDescendantsThroughTopmost(dsInfo).map { descendant =>
      val (_, sameAs) = descendant
      for {
        _          <- cleanUpTopmostSameAs(descendant)
        newTopmost <- findTopmostSameAsOnDsWith(sameAs)
        _          <- insertNewTopmostSameAs(descendant, newTopmost)
      } yield ()
    }.sequence

  private def findTopmostSameAsOnDsWith(sameAs: SameAs): F[TopmostSameAs] = {
    implicit val decoder: Decoder[List[TopmostSameAs]] =
      ResultsDecoder[TopmostSameAs](implicit cursor => extract[TopmostSameAs]("topmostSameAs"))

    queryExpecting[List[TopmostSameAs]] {
      SparqlQuery.of(
        name = "find TopmostSameAs by SameAs",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""
        SELECT ?topmostSameAs
        WHERE {
          ?dsId schema:sameAs <${sameAs.show}>;
                renku:topmostSameAs ?topmostSameAs.
        }
        LIMIT 1
        """
      )
    } >>= {
      case Nil         => new Exception(show"No topmostSameAs on DS with sameAs $sameAs").raiseError[F, TopmostSameAs]
      case head :: Nil => head.pure[F]
      case _ => new Exception(show"Multiple topmostSameAs on DS with sameAs $sameAs").raiseError[F, TopmostSameAs]
    }
  }

  private implicit class NestedOps[O](nested: Nested[F, List, F[O]]) {
    lazy val sequence: F[Unit] = nested.value.flatMap(_.sequence).void
  }
}
