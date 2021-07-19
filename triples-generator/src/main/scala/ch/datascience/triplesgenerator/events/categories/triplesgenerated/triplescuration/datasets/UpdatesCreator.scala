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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import ch.datascience.graph.model.Schemas.{renku, schema}
import ch.datascience.graph.model.datasets
import ch.datascience.graph.model.datasets.{ResourceId, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.graph.model.entities.Dataset
import ch.datascience.graph.model.entities.Dataset.Provenance
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import eu.timepit.refined.auto._

private trait UpdatesCreator {

  def prepareUpdates(dataset: Dataset[Dataset.Provenance.Internal])(implicit
      ev:                     Dataset.Provenance.Internal.type
  ): List[SparqlQuery]

  def prepareUpdates(dataset: Dataset[Dataset.Provenance.ImportedExternal])(implicit
      ev:                     Dataset.Provenance.ImportedExternal.type
  ): List[SparqlQuery]

  def prepareUpdates(dataset: Dataset[Dataset.Provenance.ImportedInternal])(implicit
      ev:                     Dataset.Provenance.ImportedInternal.type
  ): List[SparqlQuery]

  def prepareUpdates(dataset:              Dataset[Dataset.Provenance.ImportedInternal],
                     maybeKGTopmostSameAs: Option[TopmostSameAs]
  )(implicit ev:                           TopmostSameAs.type): List[SparqlQuery]

  def prepareUpdates(dataset:                   Dataset[Dataset.Provenance.Modified],
                     maybeKGTopmostDerivedFrom: Option[TopmostDerivedFrom]
  )(implicit ev:                                TopmostDerivedFrom.type): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  def prepareUpdates(
      dataset:   Dataset[Dataset.Provenance.Internal]
  )(implicit ev: Dataset.Provenance.Internal.type): List[SparqlQuery] =
    Option
      .when(dataset.maybeInvalidationTime.isDefined) {
        List(useTopmostSameAsFromTheOldestDeletedDSChildOnAncestors(dataset), deleteSameAs(dataset))
      }
      .toList
      .flatten

  def prepareUpdates(
      dataset:   Dataset[Dataset.Provenance.ImportedExternal]
  )(implicit ev: Dataset.Provenance.ImportedExternal.type): List[SparqlQuery] =
    Option
      .when(dataset.maybeInvalidationTime.isDefined)(useDeletedDSSameAsAsChildSameAs(dataset))
      .toList

  def prepareUpdates(dataset: Dataset[Dataset.Provenance.ImportedInternal])(implicit
      ev:                     Dataset.Provenance.ImportedInternal.type
  ): List[SparqlQuery] =
    Option
      .when(dataset.maybeInvalidationTime.isDefined)(useDeletedDSSameAsAsChildSameAs(dataset))
      .toList

  override def prepareUpdates(dataset:              Dataset[Provenance.ImportedInternal],
                              maybeKGTopmostSameAs: Option[TopmostSameAs]
  )(implicit ev:                                    datasets.TopmostSameAs.type): List[SparqlQuery] =
    Option
      .when(
        !(maybeKGTopmostSameAs contains dataset.provenance.topmostSameAs)
          && dataset.maybeInvalidationTime.isEmpty
      )(prepareSameAsUpdate(dataset.resourceId, dataset.provenance.topmostSameAs))
      .toList

  override def prepareUpdates(dataset:                   Dataset[Provenance.Modified],
                              maybeKGTopmostDerivedFrom: Option[TopmostDerivedFrom]
  )(implicit ev:                                         datasets.TopmostDerivedFrom.type): List[SparqlQuery] =
    Option
      .when(
        !(maybeKGTopmostDerivedFrom contains dataset.provenance.topmostDerivedFrom) &&
          dataset.maybeInvalidationTime.isEmpty
      )(prepareDerivedFromUpdate(dataset.resourceId, dataset.provenance.topmostDerivedFrom))
      .toList

  private def deleteSameAs(dataset: Dataset[Provenance.Internal]) = SparqlQuery.of(
    name = "transformation - delete sameAs",
    Prefixes.of(schema -> "schema"),
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

  private def prepareDerivedFromUpdate(oldTopmostDerivedFrom: ResourceId, newTopmostDerivedFrom: TopmostDerivedFrom) =
    SparqlQuery.of(
      name = "transformation - topmostDerivedFrom update",
      Prefixes.of(renku -> "renku", schema -> "schema"),
      s"""|DELETE { ?dsId renku:topmostDerivedFrom <$oldTopmostDerivedFrom> }
          |INSERT { ?dsId renku:topmostDerivedFrom <$newTopmostDerivedFrom> }
          |WHERE {
          |  ?dsId a schema:Dataset;
          |        renku:topmostDerivedFrom <$oldTopmostDerivedFrom>
          |}
          |""".stripMargin
    )
}
