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

package io.renku.triplesgenerator.projects.update

import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import eu.timepit.refined.auto._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.Schemas.{rdf, renku, schema}
import io.renku.graph.model.images.{Image, ImageUri}
import io.renku.graph.model.{GraphClass, RenkuUrl, projects}
import io.renku.jsonld.syntax._
import io.renku.projectauth.ProjectAuth
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesgenerator.api.ProjectUpdates
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait UpdateQueriesCalculator[F[_]] {
  def calculateUpdateQueries(slug: projects.Slug, updates: ProjectUpdates): F[List[SparqlQuery]]
}

private object UpdateQueriesCalculator {
  def apply[F[_]: MonadThrow: Logger](): F[UpdateQueriesCalculator[F]] =
    RenkuUrlLoader[F]().map(implicit ru => new UpdateQueriesCalculatorImpl[F])
}

private class UpdateQueriesCalculatorImpl[F[_]: Applicative: Logger](implicit ru: RenkuUrl)
    extends UpdateQueriesCalculator[F] {

  def calculateUpdateQueries(slug: projects.Slug, updates: ProjectUpdates): F[List[SparqlQuery]] =
    logUpdateStatus(slug, updates)
      .map(_ => calculateUpdates(slug, updates))

  private def calculateUpdates(slug: projects.Slug, updates: ProjectUpdates) =
    (
      updates.newDescription.map(descUpdates(slug, _)) combine
        updates.newKeywords.map(keywordsUpdates(slug, _)) combine
        updates.newImages.map(imagesUpdates(slug, _)) combine
        updates.newVisibility.map(visibilityUpdates(slug, _))
    ).getOrElse(Nil)

  private def descUpdates(slug: projects.Slug, newValue: Option[projects.Description]): List[SparqlQuery] = List(
    descInProjectUpdate(slug, newValue),
    descInProjectsUpdate(slug, newValue)
  )

  private def descInProjectUpdate(slug: projects.Slug, newValue: Option[projects.Description]) =
    newValue match {
      case Some(value) =>
        SparqlQuery.ofUnsafe(
          show"$reportingPrefix: update desc in Project",
          Prefixes of (renku -> "renku", schema -> "schema"),
          sparql"""|DELETE { GRAPH ?id { ?id schema:description ?maybeDesc } }
                   |INSERT { GRAPH ?id { ?id schema:description ${value.asObject} } }
                   |WHERE {
                   |  GRAPH ?id {
                   |    ?id a schema:Project;
                   |        renku:slug ${slug.asObject}.
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
      case None =>
        SparqlQuery.ofUnsafe(
          show"$reportingPrefix: delete desc in Project",
          Prefixes of (renku -> "renku", schema -> "schema"),
          sparql"""|DELETE { GRAPH ?id { ?id schema:description ?maybeDesc } }
                   |WHERE {
                   |  GRAPH ?id {
                   |    ?id a schema:Project;
                   |        renku:slug ${slug.asObject}.
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
    }

  private def descInProjectsUpdate(slug: projects.Slug, newValue: Option[projects.Description]) =
    newValue match {
      case Some(value) =>
        SparqlQuery.ofUnsafe(
          show"$reportingPrefix: update desc in Projects",
          Prefixes of (renku -> "renku", schema -> "schema"),
          sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:description ?maybeDesc } }
                   |INSERT { GRAPH ${GraphClass.Projects.id} { ?id schema:description ${value.asObject} } }
                   |WHERE {
                   |  GRAPH ${GraphClass.Projects.id} {
                   |    ?id renku:slug ${slug.asObject}.
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
      case None =>
        SparqlQuery.ofUnsafe(
          show"$reportingPrefix: delete desc in Projects",
          Prefixes of (renku -> "renku", schema -> "schema"),
          sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:description ?maybeDesc } }
                   |WHERE {
                   |  GRAPH ${GraphClass.Projects.id} {
                   |    ?id renku:slug ${slug.asObject}.
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
    }

  private def keywordsUpdates(slug: projects.Slug, newValue: Set[projects.Keyword]): List[SparqlQuery] = List(
    keywordsInProjectUpdate(slug, newValue),
    keywordsInProjectsUpdate(slug, newValue)
  )

  private def keywordsInProjectUpdate(slug: projects.Slug, newValue: Set[projects.Keyword]) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update keywords in Project",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE { GRAPH ?id { ?id schema:keywords ?keyword } }
               |INSERT { GRAPH ?id {
               |  ${newValue.map(k => fr"""?id schema:keywords ${k.asObject}.""").toList.intercalate(fr"\n ")}
               |} }
               |WHERE {
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:slug ${slug.asObject}.
               |    OPTIONAL { ?id schema:keywords ?keyword }
               |  }
               |}""".stripMargin
    )

  private def keywordsInProjectsUpdate(slug: projects.Slug, newValue: Set[projects.Keyword]) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update keywords in Projects",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:keywords ?keyword } }
               |INSERT { GRAPH ${GraphClass.Projects.id} {
               |  ${newValue.map(k => fr"""?id schema:keywords ${k.asObject}.""").toList.intercalate(fr"\n")}
               |} }
               |WHERE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id renku:slug ${slug.asObject}.
               |    OPTIONAL { ?id schema:keywords ?keyword }
               |  }
               |}""".stripMargin
    )

  private def imagesUpdates(slug: projects.Slug, newValue: List[ImageUri]): List[SparqlQuery] = {
    val newImages = Image.projectImage(projects.ResourceId(slug), newValue)
    List(
      imagesInProjectUpdate(slug, newImages),
      imagesInProjectsUpdate(slug, newImages)
    )
  }

  private def imagesInProjectUpdate(slug: projects.Slug, newValue: List[Image]) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update images in Project",
      Prefixes of (rdf -> "rdf", renku -> "renku", schema -> "schema"),
      sparql"""|DELETE { GRAPH ?id {
               |  ?id schema:image ?imageId.
               |  ?imageId ?p ?o
               |} }
               |INSERT { GRAPH ?id {
               |  ${newValue.flatMap(toTriple).intercalate(fr"\n ")}
               |} }
               |WHERE {
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:slug ${slug.asObject}.
               |    OPTIONAL {
               |      ?id schema:image ?imageId.
               |      ?imageId ?p ?o
               |    }
               |  }
               |}""".stripMargin
    )

  private def imagesInProjectsUpdate(slug: projects.Slug, newValue: List[Image]) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update keywords in Projects",
      Prefixes of (rdf -> "rdf", renku -> "renku", schema -> "schema"),
      sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} {
               |  ?id schema:image ?imageId.
               |  ?imageId ?p ?o
               |} }
               |INSERT { GRAPH ${GraphClass.Projects.id} {
               |  ${newValue.flatMap(toTriple).intercalate(fr"\n ")}
               |} }
               |WHERE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id renku:slug ${slug.asObject}.
               |    OPTIONAL {
               |      ?id schema:image ?imageId.
               |      ?imageId ?p ?o
               |    }
               |  }
               |}""".stripMargin
    )

  private lazy val toTriple: Image => List[Fragment] = { case Image(resourceId, uri, position) =>
    List(
      fr"""?id schema:image ${resourceId.asEntityId}.""",
      fr"""${resourceId.asEntityId} rdf:type ${Image.Ontology.typeClass.id}.""",
      fr"""${resourceId.asEntityId} ${Image.Ontology.contentUrlProperty.id} ${uri.asObject}.""",
      fr"""${resourceId.asEntityId} ${Image.Ontology.positionProperty.id} ${position.asObject}."""
    )
  }

  private def visibilityUpdates(slug: projects.Slug, newValue: projects.Visibility): List[SparqlQuery] = List(
    visibilityInProjectUpdate(slug, newValue),
    visibilityInProjectsUpdate(slug, newValue),
    visibilityInProjectAuthUpdate(slug, newValue)
  )

  private def visibilityInProjectAuthUpdate(slug: projects.Slug, newValue: projects.Visibility) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update visibility in ProjectAuth",
      Prefixes of (renku -> "renku", schema -> "schema"),
      SparqlSnippets.changeVisibility(slug, newValue)
    )

  private def visibilityInProjectUpdate(slug: projects.Slug, newValue: projects.Visibility) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update visibility in Project",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE { GRAPH ?id { ?id renku:projectVisibility ?visibility } }
               |INSERT { GRAPH ?id { ?id renku:projectVisibility ${newValue.asObject} } }
               |WHERE {
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:slug ${slug.asObject};
               |        renku:projectVisibility ?visibility.
               |  }
               |}""".stripMargin
    )

  private def visibilityInProjectsUpdate(slug: projects.Slug, newValue: projects.Visibility) =
    SparqlQuery.ofUnsafe(
      show"$reportingPrefix: update visibility in Projects",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id renku:projectVisibility ?visibility } }
               |INSERT { GRAPH ${GraphClass.Projects.id} { ?id renku:projectVisibility ${newValue.asObject} } }
               |WHERE {
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id renku:slug ${slug.asObject};
               |        renku:projectVisibility ?visibility.
               |  }
               |}""".stripMargin
    )

  private def logUpdateStatus(slug: projects.Slug, updates: ProjectUpdates) =
    List(
      updates.newDescription.map(v => show"description to ${v.fold("null")(vv => show"'$vv'")}"),
      updates.newKeywords.map(v => show"keywords to '${v.mkString(", ")}'"),
      updates.newImages.map(v => show"images to '${v.mkString(", ")}'"),
      updates.newVisibility.map(v => show"visibility to '$v'")
    ).flatten match {
      case Nil     => ().pure[F]
      case updates => Logger[F].info(show"$reportingPrefix: updating ${updates.mkString(", ")} for project $slug")
    }
}
