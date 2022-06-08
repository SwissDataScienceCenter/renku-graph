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

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.datasets.{DateCreated, Description, OriginalIdentifier, ResourceId, SameAs, TopmostSameAs}
import io.renku.graph.model.entities.Dataset
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.persons
import io.renku.graph.model.views.RdfResource
import io.renku.jsonld.syntax._
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

  def prepareUpdates(dataset:                Dataset[Dataset.Provenance.ImportedInternal],
                     maybeKGTopmostSameAses: Set[TopmostSameAs]
  ): List[SparqlQuery]

  def prepareTopmostSameAsCleanup(dataset:                  Dataset[Dataset.Provenance.ImportedInternal],
                                  maybeParentTopmostSameAs: Option[TopmostSameAs]
  ): List[SparqlQuery]

  def queriesUnlinkingCreators(dataset:      Dataset[Dataset.Provenance],
                               creatorsInKG: Set[persons.ResourceId]
  ): List[SparqlQuery]

  def deleteOtherDerivedFrom(dataset: Dataset[Dataset.Provenance.Modified]): List[SparqlQuery]

  def deleteOtherTopmostDerivedFrom(dataset: Dataset[Dataset.Provenance.Modified]): List[SparqlQuery]

  def removeOtherOriginalIdentifiers(dataset:                 Dataset[Dataset.Provenance],
                                     originalIdentifiersInKG: Set[OriginalIdentifier]
  ): List[SparqlQuery]

  def removeOtherDateCreated(dataset: Dataset[Dataset.Provenance], dateCreatedInKG: Set[DateCreated]): List[SparqlQuery]

  def removeOtherDescriptions(dataset: Dataset[Dataset.Provenance], descsInKG: Set[Description]): List[SparqlQuery]

  def removeOtherSameAs(dataset: Dataset[Dataset.Provenance], sameAsInKG: Set[SameAs]): List[SparqlQuery]
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

  override def prepareUpdates(dataset:                Dataset[Provenance.ImportedInternal],
                              maybeKGTopmostSameAses: Set[TopmostSameAs]
  ): List[SparqlQuery] = Option
    .when(!(maybeKGTopmostSameAses equals Set(dataset.provenance.topmostSameAs)))(
      SparqlQuery.of(
        name = "transformation - topmostSameAs update",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""|DELETE { ?dsId renku:topmostSameAs ?oldTopmost }
            |INSERT { ?dsId renku:topmostSameAs <${dataset.provenance.topmostSameAs}> }
            |WHERE {
            |  ?dsId a schema:Dataset;
            |        renku:topmostSameAs <${dataset.resourceId}>;
            |        renku:topmostSameAs ?oldTopmost.
            |}
            |""".stripMargin
      )
    )
    .toList

  override def prepareTopmostSameAsCleanup(dataset:                  Dataset[Dataset.Provenance.ImportedInternal],
                                           maybeParentTopmostSameAs: Option[TopmostSameAs]
  ): List[SparqlQuery] = maybeParentTopmostSameAs match {
    case None    => Nil
    case Some(_) => List(prepareTopmostSameAsCleanUp(dataset.resourceId, dataset.provenance.topmostSameAs))
  }

  override def queriesUnlinkingCreators(dataset:      Dataset[Dataset.Provenance],
                                        creatorsInKG: Set[persons.ResourceId]
  ): List[SparqlQuery] = {
    val dsCreators = dataset.provenance.creators.map(_.resourceId).toList.toSet
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

  override def deleteOtherDerivedFrom(dataset: Dataset[Dataset.Provenance.Modified]): List[SparqlQuery] = List(
    SparqlQuery.of(
      name = "transformation - delete other derivedFrom",
      Prefixes of (prov -> "prov", schema -> "schema"),
      s"""|DELETE {
          |  ${dataset.resourceId.showAs[RdfResource]} prov:wasDerivedFrom ?derivedId
          |}
          |WHERE {
          |  ${dataset.resourceId.showAs[RdfResource]} a schema:Dataset;
          |                                            prov:wasDerivedFrom ?derivedId.
          |  ?derivedId schema:url ?derived. 
          |  FILTER (?derived != ${dataset.provenance.derivedFrom.showAs[RdfResource]})
          |}
          |""".stripMargin
    )
  )

  override def deleteOtherTopmostDerivedFrom(
      dataset: Dataset[Dataset.Provenance.Modified]
  ): List[SparqlQuery] = List(
    SparqlQuery.of(
      name = "transformation - delete other topmostDerivedFrom",
      Prefixes of (renku -> "renku", schema -> "schema"),
      s"""|DELETE {
          |  ${dataset.resourceId.showAs[RdfResource]} renku:topmostDerivedFrom ?topmostDerived
          |}
          |WHERE {
          |  ${dataset.resourceId.showAs[RdfResource]} a schema:Dataset;
          |                                            renku:topmostDerivedFrom ?topmostDerived.
          |  FILTER (?topmostDerived != ${dataset.provenance.topmostDerivedFrom.showAs[RdfResource]})
          |}
          |""".stripMargin
    )
  )

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

  override def removeOtherOriginalIdentifiers(ds:                      Dataset[Provenance],
                                              originalIdentifiersInKG: Set[OriginalIdentifier]
  ) = Option
    .when((originalIdentifiersInKG - ds.provenance.originalIdentifier).nonEmpty) {
      SparqlQuery.of(
        name = "transformation - originalIdentifier clean-up",
        Prefixes of renku -> "renku",
        s"""|DELETE { ?dsId renku:originalIdentifier ?origIdentifier }
            |WHERE {
            |  BIND (${ds.resourceId.showAs[RdfResource]} AS ?dsId)
            |  ?dsId renku:originalIdentifier ?origIdentifier.
            |  FILTER ( ?origIdentifier != '${ds.provenance.originalIdentifier}' )
            |}""".stripMargin
      )
    }
    .toList

  override def removeOtherDateCreated(ds:              Dataset[Dataset.Provenance],
                                      dateCreatedInKG: Set[DateCreated]
  ): List[SparqlQuery] = Option
    .when(
      ds.provenance.date.isInstanceOf[DateCreated] &&
        (dateCreatedInKG - ds.provenance.date.asInstanceOf[DateCreated]).nonEmpty
    ) {
      SparqlQuery.of(
        name = "transformation - dateCreated clean-up",
        Prefixes of schema -> "schema",
        s"""|DELETE { ?dsId schema:dateCreated ?date }
            |WHERE {
            |  BIND (${ds.resourceId.showAs[RdfResource]} AS ?dsId)
            |  ?dsId schema:dateCreated ?date.
            |  FILTER ( STR(?date) != '${ds.provenance.date}' )
            |}""".stripMargin
      )
    }
    .toList

  def removeOtherDescriptions(ds: Dataset[Dataset.Provenance], descsInKG: Set[Description]): List[SparqlQuery] =
    ds.additionalInfo.maybeDescription match {
      case Some(desc) if (descsInKG - desc).nonEmpty =>
        List(
          SparqlQuery.of(
            name = "transformation - ds desc clean-up",
            Prefixes of schema -> "schema",
            s"""|DELETE { ?dsId schema:description ?desc }
                |WHERE {
                |  BIND (${ds.resourceId.showAs[RdfResource]} AS ?dsId)
                |  ?dsId schema:description ?desc.
                |  FILTER ( ?desc != '$desc' )
                |}""".stripMargin
          )
        )
      case None if descsInKG.nonEmpty =>
        List(
          SparqlQuery.of(
            name = "transformation - ds desc remove",
            Prefixes of schema -> "schema",
            s"""|DELETE { ?dsId schema:description ?desc }
                |WHERE {
                |  BIND (${ds.resourceId.showAs[RdfResource]} AS ?dsId)
                |  ?dsId schema:description ?desc.
                |}""".stripMargin
          )
        )
      case _ => List.empty
    }

  override def removeOtherSameAs(ds: Dataset[Dataset.Provenance], sameAsInKG: Set[SameAs]): List[SparqlQuery] = {
    val maybeSameAs = {
      ds.provenance match {
        case p: Dataset.Provenance.ImportedExternal => p.sameAs.some
        case p: Dataset.Provenance.ImportedInternal => p.sameAs.some
        case _ => None
      }
    }.widen[SameAs] >>= (_.asJsonLD.entityId)

    maybeSameAs match {
      case None => Nil
      case Some(dsSameAs) =>
        Option
          .when((sameAsInKG.map(_.show) - dsSameAs.show).nonEmpty) {
            SparqlQuery.of(
              name = "transformation - sameAs clean-up",
              Prefixes of schema -> "schema",
              s"""|DELETE { ?dsId schema:sameAs ?sameAs }
                  |WHERE {
                  |  BIND (${ds.resourceId.showAs[RdfResource]} AS ?dsId)
                  |  ?dsId schema:sameAs ?sameAs.
                  |  FILTER ( ?sameAs != <${dsSameAs.show}> )
                  |}""".stripMargin
            )
          }
          .toList
    }
  }
}
