package io.renku.graph.model.cli

import cats.syntax.option._
import io.renku.graph.model.datasets.{DateModified, OriginalIdentifier}
import io.renku.graph.model.entities
import io.renku.graph.model.entities.Dataset.Identification

trait CliDatasetProvenanceConv {

  def from(id: Identification, prov: entities.Dataset.Provenance): CliDatasetProvenance =
    prov match {
      case entities.Dataset.Provenance.Internal(resourceId, identifier, created, creators) =>
        CliDatasetProvenance(
          id = Identification(resourceId, identifier, id.title, id.name),
          creators = creators,
          createdAt = created.some,
          publishedAt = None,
          modifiedAt = None,
          sameAs = None,
          derivedFrom = None,
          originalIdentifier = OriginalIdentifier(identifier).some,
          invalidationTime = None
        )

      case entities.Dataset.Provenance.ImportedExternal(resourceId, identifier, sameAs, published, creators) =>
        CliDatasetProvenance(
          id = Identification(resourceId, identifier, id.title, id.name),
          creators = creators,
          createdAt = None,
          publishedAt = published.some,
          modifiedAt = None,
          sameAs = CliDatasetSameAs(sameAs.value).some,
          derivedFrom = None,
          originalIdentifier = OriginalIdentifier(identifier.value).some,
          invalidationTime = None
        )

      case entities.Dataset.Provenance.ImportedInternalAncestorExternal(
            resourceId,
            identifier,
            sameAs,
            _,
            originalIdentifier,
            published,
            creators
          ) =>
        CliDatasetProvenance(
          id = Identification(resourceId, identifier, id.title, id.name),
          creators = creators,
          createdAt = None,
          publishedAt = published.some,
          modifiedAt = None,
          sameAs = CliDatasetSameAs(sameAs.value).some,
          derivedFrom = None,
          originalIdentifier = originalIdentifier.some,
          invalidationTime = None
        )

      case entities.Dataset.Provenance.ImportedInternalAncestorInternal(
            resourceId,
            identifier,
            sameAs,
            _,
            originalIdentifier,
            created,
            creators
          ) =>
        CliDatasetProvenance(
          id = Identification(resourceId, identifier, id.title, id.name),
          creators = creators,
          createdAt = created.some,
          publishedAt = None,
          modifiedAt = None,
          sameAs = CliDatasetSameAs(sameAs.value).some,
          derivedFrom = None,
          originalIdentifier = originalIdentifier.some,
          invalidationTime = None
        )

      case entities.Dataset.Provenance.Modified(
            resourceId,
            derivedFrom,
            _,
            originalIdentifier,
            created,
            creators,
            maybeInvalidationTime
          ) =>
        CliDatasetProvenance(
          id = Identification(resourceId, id.identifier, id.title, id.name),
          creators = creators,
          createdAt = None,
          publishedAt = None,
          modifiedAt = DateModified(created.value).some,
          sameAs = None,
          derivedFrom = derivedFrom.some,
          originalIdentifier = originalIdentifier.some,
          invalidationTime = maybeInvalidationTime
        )
    }
}

object CliDatasetProvenanceConv extends CliDatasetProvenanceConv
