package io.renku.graph.model.cli

import io.renku.generators.Generators
import io.renku.graph.model.{GraphModelGenerators, RenkuUrl}
import io.renku.graph.model.datasets.{DerivedFrom, OriginalIdentifier, TopmostDerivedFrom, TopmostSameAs}
import io.renku.graph.model.projects.{DateCreated => ProjectDateCreated}
import io.renku.graph.model.testentities.{Dataset, Person, ProvenanceGen}
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import org.scalacheck.Gen

object CliGenerators {

  private val cliCompatibleTestPerson: Gen[Person] =
    EntitiesGenerators.personEntities(maybeGitLabIds = Gen.const(None))

  private val provenanceInternal: ProvenanceGen[Dataset.Provenance.Internal] = (identifier, projectDateCreated) =>
    implicit renkuUrl =>
      for {
        date     <- GraphModelGenerators.datasetCreatedDates(projectDateCreated.value)
        creators <- Generators.nonEmptyList(cliCompatibleTestPerson, max = 1)
      } yield Dataset.Provenance.Internal(
        Dataset.entityId(identifier),
        OriginalIdentifier(identifier),
        date,
        creators.sortBy(_.name)
      )

  private val provenanceImportedInternalAncestorExternal
      : ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorExternal] =
    (identifier, _) =>
      implicit renkuUrl =>
        for {
          date   <- GraphModelGenerators.datasetPublishedDates()
          sameAs <- GraphModelGenerators.datasetInternalSameAs
          originalId <-
            Gen.oneOf(Gen.const(OriginalIdentifier(identifier)), GraphModelGenerators.datasetOriginalIdentifiers)
          creators <- Generators.nonEmptyList(cliCompatibleTestPerson, max = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorExternal(
          Dataset.entityId(identifier),
          sameAs,
          TopmostSameAs(sameAs),
          originalId,
          date,
          creators.sortBy(_.name)
        )

  private val provenanceImportedInternalAncestorInternal
      : ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorInternal] =
    (identifier, projectDateCreated) =>
      implicit renkuUrl =>
        for {
          date   <- GraphModelGenerators.datasetCreatedDates(projectDateCreated.value)
          sameAs <- GraphModelGenerators.datasetInternalSameAs
          originalId <-
            Gen.oneOf(Gen.const(OriginalIdentifier(identifier)), GraphModelGenerators.datasetOriginalIdentifiers)
          creators <- Generators.nonEmptyList(cliCompatibleTestPerson, max = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorInternal(Dataset.entityId(identifier),
                                                                    sameAs,
                                                                    TopmostSameAs(sameAs),
                                                                    originalId,
                                                                    date,
                                                                    creators.sortBy(_.name)
        )

  private val provenanceImportedExternal: ProvenanceGen[Dataset.Provenance.ImportedExternal] = (identifier, _) =>
    implicit renkuUrl =>
      for {
        date     <- GraphModelGenerators.datasetPublishedDates()
        sameAs   <- GraphModelGenerators.datasetExternalSameAs
        creators <- Generators.nonEmptyList(cliCompatibleTestPerson, max = 1)
      } yield Dataset.Provenance.ImportedExternal(
        Dataset.entityId(identifier),
        sameAs,
        OriginalIdentifier(identifier),
        date,
        creators.sortBy(_.name)
      )

  private val provenanceModified: ProvenanceGen[Dataset.Provenance.Modified] = (identifier, projectDateCreated) =>
    implicit renkuUrl =>
      for {
        date <- GraphModelGenerators.datasetCreatedDates(projectDateCreated.value)
        originalId <-
          Gen.oneOf(Gen.const(OriginalIdentifier(identifier)), GraphModelGenerators.datasetOriginalIdentifiers)
        creators         <- Generators.nonEmptyList(cliCompatibleTestPerson, max = 1)
        derived          <- GraphModelGenerators.datasetIdentifiers
        invalidationTime <- Gen.option(EntitiesGenerators.invalidationTimes(projectDateCreated.value))
      } yield Dataset.Provenance.Modified(
        Dataset.entityId(identifier),
        DerivedFrom(Dataset.entityId(derived)),
        TopmostDerivedFrom(Dataset.entityId(derived)),
        originalId,
        date,
        creators,
        invalidationTime
      )

  private val provenanceGen: ProvenanceGen[Dataset.Provenance] = {
    val gens: List[ProvenanceGen[Dataset.Provenance]] =
      List(
        provenanceInternal.asInstanceOf[ProvenanceGen[Dataset.Provenance]],
        provenanceImportedInternalAncestorExternal,
        provenanceImportedInternalAncestorInternal,
        provenanceImportedExternal,
        provenanceModified
      )

    (id, projectDate) => renkuUrl => Gen.oneOf(gens).flatMap(_(id, projectDate)(renkuUrl))
  }

  def personGen: Gen[CliPerson] =
    cliCompatibleTestPerson.map(_.to[CliPerson])

  def datasetGen(
      dateCreatedGen:  Gen[ProjectDateCreated] = GraphModelGenerators.projectCreatedDates()
  )(implicit renkuUrl: RenkuUrl): Gen[CliDataset] = {
    val datasets = EntitiesGenerators.datasetEntities(provenanceGen)
    dateCreatedGen.flatMap(datasets).map(_.to[CliDataset])
  }
}
