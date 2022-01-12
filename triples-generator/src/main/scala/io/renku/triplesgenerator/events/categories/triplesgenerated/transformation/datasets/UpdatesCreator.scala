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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.datasets

import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.datasets.{ResourceId, TopmostSameAs}
import io.renku.graph.model.entities.Dataset
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.users
import io.renku.graph.model.views.RdfResource
import io.renku.rdfstore.SparqlQuery
import io.renku.rdfstore.SparqlQuery.Prefixes

private trait UpdatesCreator {

  def prepareUpdatesWhenInvalidated(dataset: Dataset[Dataset.Provenance.Internal])(implicit
      ev:                                    Dataset.Provenance.Internal.type
  ): List[SparqlQuery]

  def prepareUpdatesWhenInvalidated(dataset: Dataset[Dataset.Provenance.ImportedExternal])(implicit
      ev:                                    Dataset.Provenance.ImportedExternal.type
  ): List[SparqlQuery]

  def prepareUpdatesWhenInvalidated(dataset: Dataset[Dataset.Provenance.ImportedInternal])(implicit
      ev:                                    Dataset.Provenance.ImportedInternal.type
  ): List[SparqlQuery]

  def prepareUpdates(dataset:              Dataset[Dataset.Provenance.ImportedInternal],
                     maybeKGTopmostSameAs: Option[TopmostSameAs]
  ): List[SparqlQuery]

  def prepareTopmostSameAsCleanup(dataset:                  Dataset[Dataset.Provenance.ImportedInternal],
                                  maybeParentTopmostSameAs: Option[TopmostSameAs]
  ): List[SparqlQuery]

  def queriesUnlinkingCreators(dataset:      Dataset[Dataset.Provenance],
                               creatorsInKG: Set[users.ResourceId]
  ): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  override def prepareUpdatesWhenInvalidated(
      dataset:   Dataset[Dataset.Provenance.Internal]
  )(implicit ev: Dataset.Provenance.Internal.type): List[SparqlQuery] =
    List(useTopmostSameAsFromTheOldestDeletedDSChildOnAncestors(dataset), deleteSameAs(dataset))

  override def prepareUpdatesWhenInvalidated(
      dataset:   Dataset[Dataset.Provenance.ImportedExternal]
  )(implicit ev: Dataset.Provenance.ImportedExternal.type): List[SparqlQuery] =
    List(useDeletedDSSameAsAsChildSameAs(dataset))

  override def prepareUpdatesWhenInvalidated(
      dataset:   Dataset[Dataset.Provenance.ImportedInternal]
  )(implicit ev: Dataset.Provenance.ImportedInternal.type): List[SparqlQuery] =
    List(useDeletedDSSameAsAsChildSameAs(dataset))

  override def prepareUpdates(dataset:              Dataset[Provenance.ImportedInternal],
                              maybeKGTopmostSameAs: Option[TopmostSameAs]
  ): List[SparqlQuery] =
    Option
      .when(!(maybeKGTopmostSameAs contains dataset.provenance.topmostSameAs))(
        prepareSameAsUpdate(dataset.resourceId, dataset.provenance.topmostSameAs)
      )
      .toList

  override def prepareTopmostSameAsCleanup(dataset:                  Dataset[Dataset.Provenance.ImportedInternal],
                                           maybeParentTopmostSameAs: Option[TopmostSameAs]
  ): List[SparqlQuery] = maybeParentTopmostSameAs match {
    case None    => Nil
    case Some(_) => List(prepareTopmostSameAsCleanUp(dataset.resourceId, dataset.provenance.topmostSameAs))
  }

  override def queriesUnlinkingCreators(dataset:      Dataset[Dataset.Provenance],
                                        creatorsInKG: Set[users.ResourceId]
  ): List[SparqlQuery] = {
    val dsCreators = dataset.provenance.creators.map(_.resourceId)
    Option
      .when(dsCreators != creatorsInKG) {
        SparqlQuery.of(
          name = "transformation - delete ds creators link",
          Prefixes of schema -> "schema",
          s"""|DELETE {
              |  ${dataset.resourceId.showAs[RdfResource]} schema:creator ?personId
              |}
              |WHERE {
              |  ${dataset.resourceId.showAs[RdfResource]} a schema:Dataset;
              |                                            schema:creator ?personId.
              |  FILTER (?personId NOT IN (${dsCreators.map(_.showAs[RdfResource]).mkString(", ")}))
              |}
              |""".stripMargin
        )
      }
      .toList
  }

  private def deleteSameAs(dataset: Dataset[Provenance.Internal]) = SparqlQuery.of(
    name = "transformation - delete sameAs",
    Prefixes of schema -> "schema",
    s"""|DELETE { 
        |  ?dsId schema:sameAs ?sameAs.
        |  ?sameAs ?sameAsPredicate ?sameAsObject.
        |}
        |WHERE {
        |  ?dsId a schema:Dataset;
        |        schema:sameAs ?sameAs.
        |  ?sameAs schema:url <${dataset.resourceId}>;
        |          ?sameAsPredicate ?sameAsObject.
        |}
        |""".stripMargin
  )

  private def useTopmostSameAsFromTheOldestDeletedDSChildOnAncestors(dataset: Dataset[Provenance.Internal]) =
    SparqlQuery.of(
      name = "transformation - topmostSameAs from child",
      Prefixes.of(renku -> "renku", schema -> "schema"),
      s"""|DELETE { 
          |  ?ancestorsDsId renku:topmostSameAs <${dataset.resourceId}>.
          |}
          |INSERT {
          |  ?ancestorsDsId renku:topmostSameAs ?oldestChildResourceId.
          |}
          |WHERE {
          |  {
          |    SELECT (?dsId AS ?oldestChildResourceId)
          |    WHERE {
          |      ?dsId a schema:Dataset;
          |            schema:dateCreated ?date;
          |            schema:sameAs ?sameAs.
          |      ?sameAs schema:url <${dataset.resourceId}>
          |    }
          |    ORDER BY ?date
          |    LIMIT 1
          |  } {
          |    ?ancestorsDsId a schema:Dataset;
          |                   renku:topmostSameAs <${dataset.resourceId}>.
          |    FILTER NOT EXISTS { ?ancestorsDsId renku:topmostSameAs ?ancestorsDsId }
          |  }
          |}
          |""".stripMargin
    )

  private def useDeletedDSSameAsAsChildSameAs(dataset: Dataset[Provenance]) =
    SparqlQuery.of(
      name = "transformation - deleted sameAs as child sameAS",
      Prefixes.of(renku -> "renku", schema -> "schema"),
      s"""|DELETE { 
          |  ?dsId schema:sameAs ?sameAs.
          |  ?sameAs ?sameAsPredicate ?sameAsObject.
          |}
          |INSERT {
          |  ?dsId schema:sameAs ?sameAsOnDeletedDS
          |}
          |WHERE {
          |  <${dataset.resourceId}> a schema:Dataset;
          |                          schema:sameAs ?sameAsOnDeletedDS.
          |  ?dsId a schema:Dataset;
          |        schema:sameAs ?sameAs.
          |  ?sameAs schema:url <${dataset.resourceId}>;
          |          ?sameAsPredicate ?sameAsObject.
          |}
          |""".stripMargin
    )

  private def prepareSameAsUpdate(oldTopmostSameAs: ResourceId, newTopmostSameAs: TopmostSameAs) = SparqlQuery.of(
    name = "transformation - topmostSameAs update",
    Prefixes.of(renku -> "renku", schema -> "schema"),
    s"""|DELETE { ?dsId renku:topmostSameAs <$oldTopmostSameAs> }
        |INSERT { ?dsId renku:topmostSameAs <$newTopmostSameAs> }
        |WHERE {
        |  ?dsId a schema:Dataset;
        |        renku:topmostSameAs <$oldTopmostSameAs>.
        |}
        |""".stripMargin
  )

  private def prepareTopmostSameAsCleanUp(dsId: ResourceId, modelTopmostSameAs: TopmostSameAs) = SparqlQuery.of(
    name = "transformation - topmostSameAs clean-up",
    Prefixes.of(renku -> "renku", schema -> "schema"),
    s"""|DELETE { <$dsId> renku:topmostSameAs <$modelTopmostSameAs> }
        |WHERE { 
        |  {
        |    SELECT (COUNT(?topmost) AS ?count)
        |    WHERE { <$dsId> renku:topmostSameAs ?topmost }
        |  }
        |  FILTER (?count > 1)
        |}
        |""".stripMargin
  )
}
