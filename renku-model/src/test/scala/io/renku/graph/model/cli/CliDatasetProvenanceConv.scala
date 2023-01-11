package io.renku.graph.model.cli

import cats.syntax.option._
import io.renku.graph.model.entities

trait CliDatasetProvenanceConv {

  def fromX(dataset: entities.Dataset[entities.Dataset.Provenance]): CliDataset =
    CliDataset(
      resourceId = dataset.identification.resourceId,
      identifier = dataset.identification.identifier,
      title = dataset.identification.title,
      name = dataset.identification.name,
      createdOrPublished = dataset.provenance.date,
      creators =
        dataset.provenance.creators.map(p => CliPerson(p.resourceId, p.name, p.maybeEmail, p.maybeAffiliation)),
      description = dataset.additionalInfo.maybeDescription,
      keywords = dataset.additionalInfo.keywords,
      images = dataset.additionalInfo.images,
      license = dataset.additionalInfo.maybeLicense,
      version = dataset.additionalInfo.maybeVersion,
      datasetFiles = dataset.parts.map(p =>
        CliDatasetFile(p.resourceId, p.external, p.entity, p.dateCreated, p.maybeSource, p.maybeInvalidationTime)
      ),
      dateModified = dataset.provenance match {
        case m: entities.Dataset.Provenance.Modified =>
          DateModified(m.date.value).some
        case _ => None
      },
      sameAs = dataset.provenance match {
        case p: entities.Dataset.Provenance.ImportedExternal =>
          CliDatasetSameAs(p.sameAs.value).some
        case p: entities.Dataset.Provenance.ImportedInternal =>
          CliDatasetSameAs(p.sameAs.value).some
        case _: entities.Dataset.Provenance.Internal =>
          None
        case _: entities.Dataset.Provenance.Modified =>
          None
      },
      derivedFrom = dataset.provenance match {
        case m: entities.Dataset.Provenance.Modified =>
          m.derivedFrom.some
        case _ => None
      },
      originalIdentifier = dataset.provenance.originalIdentifier.some,
      invalidationTime = dataset.provenance match {
        case m: entities.Dataset.Provenance.Modified =>
          m.maybeInvalidationTime
        case _ => None
      }
    )
}

object CliDatasetProvenanceConv extends CliDatasetProvenanceConv
