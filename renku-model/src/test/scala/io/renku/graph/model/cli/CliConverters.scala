package io.renku.graph.model.cli

import cats.syntax.option._
import io.renku.cli.model._
import io.renku.graph.model.{datasets, entities}

/** Conversion functions for production model entities into cli entities. */
trait CliConverters {

  def from(dataset: entities.Dataset[entities.Dataset.Provenance]): CliDataset =
    CliDataset(
      resourceId = dataset.identification.resourceId,
      identifier = dataset.identification.identifier,
      title = dataset.identification.title,
      name = dataset.identification.name,
      createdOrPublished = dataset.provenance.date,
      creators = dataset.provenance.creators.map(from),
      description = dataset.additionalInfo.maybeDescription,
      keywords = dataset.additionalInfo.keywords,
      images = dataset.additionalInfo.images,
      license = dataset.additionalInfo.maybeLicense,
      version = dataset.additionalInfo.maybeVersion,
      datasetFiles = dataset.parts.map(from),
      dateModified = dataset.provenance match {
        case p: entities.Dataset.Provenance.Modified => datasets.DateModified(p.date).some
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

  def from(part: entities.DatasetPart): CliDatasetFile =
    CliDatasetFile(part.resourceId,
                   part.external,
                   from(part.entity),
                   part.dateCreated,
                   part.maybeSource,
                   part.maybeInvalidationTime
    )

  def from(entity: entities.Entity): CliEntity = entity match {
    case entities.Entity.InputEntity(id, location, checksum) =>
      CliEntity(id, EntityPath(location.value), checksum, generationIds = Nil)
    case entities.Entity.OutputEntity(id, location, checksum, generationIds) =>
      CliEntity(id, EntityPath(location.value), checksum, generationIds)
  }

  def from(person: entities.Person): CliPerson = {
    person.maybeGitLabId.map(_ => throw new Exception(s"Cannot convert Person with GitLabId to CliPerson"))
    CliPerson(person.resourceId, person.name, person.maybeEmail, person.maybeAffiliation)
  }
}

object CliConverters extends CliConverters
