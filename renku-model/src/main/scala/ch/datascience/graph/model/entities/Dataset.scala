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

package ch.datascience.graph.model.entities

import Dataset._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.{GitLabApiUrl, InvalidationTime}

final case class Dataset[+P <: Provenance](identification:        Identification,
                                           provenance:            P,
                                           additionalInfo:        AdditionalInfo,
                                           publishing:            Publishing,
                                           parts:                 List[DatasetPart],
                                           project:               Project[ForksCount],
                                           maybeInvalidationTime: Option[InvalidationTime]
) {
  val resourceId: ResourceId = identification.resourceId
}

object Dataset {

  import ch.datascience.graph.model.Schemas._
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  final case class Identification(
      resourceId: ResourceId,
      identifier: Identifier,
      title:      Title,
      name:       Name
  )

  object Identification {
    private[Dataset] implicit lazy val encoder: Identification => Map[Property, JsonLD] = {
      case Identification(_, identifier, title, name) =>
        Map(
          schema / "identifier"    -> identifier.asJsonLD,
          schema / "name"          -> title.asJsonLD,
          schema / "alternateName" -> name.asJsonLD
        )
    }
  }

  sealed trait Provenance extends Product with Serializable {
    type D <: Date
    val topmostSameAs:      TopmostSameAs
    val initialVersion:     InitialVersion
    val topmostDerivedFrom: TopmostDerivedFrom
    val date:               D
    val creators:           Set[Person]
  }

  object Provenance {

    final case class Internal(resourceId: ResourceId, identifier: Identifier, date: DateCreated, creators: Set[Person])
        extends Provenance {
      override type D = DateCreated
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(resourceId.asEntityId)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    final case class ImportedExternal(resourceId: ResourceId,
                                      identifier: Identifier,
                                      sameAs:     ExternalSameAs,
                                      date:       DatePublished,
                                      creators:   Set[Person]
    ) extends Provenance {
      override type D = DatePublished
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(sameAs)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    sealed trait ImportedInternal extends Provenance {
      val resourceId:    ResourceId
      val identifier:    Identifier
      val sameAs:        InternalSameAs
      val topmostSameAs: TopmostSameAs
      val date:          D
      val creators:      Set[Person]

      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
    }

    final case class ImportedInternalAncestorExternal(resourceId:    ResourceId,
                                                      identifier:    Identifier,
                                                      sameAs:        InternalSameAs,
                                                      topmostSameAs: TopmostSameAs,
                                                      date:          DatePublished,
                                                      creators:      Set[Person]
    ) extends ImportedInternal {
      override type D = DatePublished
    }

    final case class ImportedInternalAncestorInternal(resourceId:    ResourceId,
                                                      identifier:    Identifier,
                                                      sameAs:        InternalSameAs,
                                                      topmostSameAs: TopmostSameAs,
                                                      date:          DateCreated,
                                                      creators:      Set[Person]
    ) extends ImportedInternal {
      override type D = DateCreated
    }

    final case class Modified(resourceId:         ResourceId,
                              derivedFrom:        DerivedFrom,
                              topmostDerivedFrom: TopmostDerivedFrom,
                              initialVersion:     InitialVersion,
                              date:               DateCreated,
                              creators:           Set[Person]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val topmostSameAs: TopmostSameAs = TopmostSameAs(resourceId.asEntityId)
    }

    private[Dataset] implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl): Provenance => Map[Property, JsonLD] = {
      case provenance @ Internal(_, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedExternal(_, sameAs, initialVersion, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorExternal(_, sameAs, topmostSameAs, initialVersion, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorInternal(_, sameAs, topmostSameAs, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ Modified(_, derivedFrom, topmostDerivedFrom, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          prov / "wasDerivedFrom"      -> derivedFrom.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
    }

    private implicit lazy val creatorsOrdering: Ordering[Person] = Ordering.by(_.name.value)
  }

  final case class Publishing(
      publicationEvents: List[PublicationEvent],
      maybeVersion:      Option[Version]
  )

  object Publishing {
    private[Dataset] implicit lazy val encoder: Publishing => Map[Property, JsonLD] = {
      case Publishing(publicationEvents, maybeVersion) =>
        Map(
          schema / "subjectOf" -> publicationEvents.asJsonLD,
          schema / "version"   -> maybeVersion.asJsonLD
        )
    }
  }

  final case class AdditionalInfo(
      url:              Url,
      maybeDescription: Option[Description],
      keywords:         List[Keyword],
      images:           List[Image],
      maybeLicense:     Option[License]
  )

  object AdditionalInfo {

    private[Dataset] implicit lazy val encoder: AdditionalInfo => Map[Property, JsonLD] = {
      case AdditionalInfo(url, maybeDescription, keywords, images, maybeLicense) =>
        Map(
          schema / "url"         -> url.asJsonLD,
          schema / "description" -> maybeDescription.asJsonLD,
          schema / "keywords"    -> keywords.asJsonLD,
          schema / "image"       -> images.asJsonLD,
          schema / "license"     -> maybeLicense.asJsonLD
        )
    }
  }

  final case class Image(resourceId: ImageResourceId, uri: ImageUri, position: ImagePosition)

  object Image {
    private[Dataset] implicit val jsonLDEncoder: JsonLDEncoder[Image] = JsonLDEncoder.instance {
      case Image(resourceId, uri, position) =>
        JsonLD.entity(
          resourceId.asEntityId,
          EntityTypes of schema / "ImageObject",
          (schema / "contentUrl") -> uri.asJsonLD,
          (schema / "position")   -> position.asJsonLD
        )
    }
  }

  implicit def encoder[P <: Provenance](implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Dataset[P]] = {
    implicit class SerializationOps[T](obj: T) {
      def asJsonLDProperties(implicit encoder: T => Map[Property, JsonLD]): Map[Property, JsonLD] = encoder(obj)
    }

    JsonLDEncoder.instance { dataset =>
      JsonLD
        .entity(
          dataset.resourceId.asEntityId,
          EntityTypes of (schema / "Dataset", prov / "Entity"),
          List(
            dataset.identification.asJsonLDProperties,
            dataset.provenance.asJsonLDProperties,
            dataset.additionalInfo.asJsonLDProperties,
            dataset.publishing.asJsonLDProperties
          ).flatten.toMap,
          schema / "hasPart"         -> dataset.parts.asJsonLD,
          schema / "isPartOf"        -> dataset.project.asJsonLD,
          prov / "invalidatedAtTime" -> dataset.maybeInvalidationTime.asJsonLD
        )
    }
  }
}
