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

package io.renku.graph.model.testentities

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.datasetIdentifiers
import io.renku.graph.model._
import io.renku.graph.model.datasets.{DateCreated, DerivedFrom, Description, InitialVersion, InternalSameAs, Keyword, Name, SameAs, Title}
import io.renku.graph.model.projects.ForksCount
import io.renku.graph.model.testentities.Dataset.Provenance
import io.renku.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._

trait ModelOps extends Dataset.ProvenanceOps {

  implicit class PersonOps(person: Person) {
    lazy val resourceId: users.ResourceId = users.ResourceId(person.asEntityId)

    def to[T](implicit convert:      Person => T):         T         = convert(person)
    def toMaybe[T](implicit convert: Person => Option[T]): Option[T] = convert(person)
  }

  implicit class ProjectWithParentOps(project: ProjectWithParent)(implicit
      renkuBaseUrl:                            RenkuBaseUrl
  ) extends AbstractProjectOps[ProjectWithParent](project)

  implicit class ProjectWithoutParentOps(project: ProjectWithoutParent)(implicit
      renkuBaseUrl:                               RenkuBaseUrl
  ) extends AbstractProjectOps[ProjectWithoutParent](project)

  implicit class ProjectOps(project: Project)(implicit
      renkuBaseUrl:                  RenkuBaseUrl
  ) extends AbstractProjectOps[Project](project)

  abstract class AbstractProjectOps[P <: Project](project: P)(implicit
      renkuBaseUrl:                                        RenkuBaseUrl
  ) {

    lazy val resourceId: projects.ResourceId = projects.ResourceId(project.asEntityId)

    def to[T](implicit convert: P => T): T = convert(project)

    def forkOnce(): (Project, ProjectWithParent) = {
      val (parent, childGen) = fork(times = 1)
      parent -> childGen.head
    }

    def fork(
        times: Int Refined Positive
    ): (Project, NonEmptyList[ProjectWithParent]) = {
      val parent = project match {
        case proj: ProjectWithParent =>
          proj.copy(forksCount = ForksCount(Refined.unsafeApply(proj.forksCount.value + times.value)))
        case proj: ProjectWithoutParent =>
          proj.copy(forksCount = ForksCount(Refined.unsafeApply(project.forksCount.value + times.value)))
      }
      parent -> (1 until times.value).foldLeft(NonEmptyList.one(newChildGen(parent).generateOne))((childrenGens, _) =>
        newChildGen(parent).generateOne :: childrenGens
      )
    }

    private def newChildGen(parentProject: Project) =
      projectEntities(fixed(parentProject.visibility), minDateCreated = parentProject.dateCreated).map(child =>
        ProjectWithParent(
          child.path,
          child.name,
          parentProject.agent,
          child.dateCreated,
          child.maybeCreator,
          child.visibility,
          ForksCount.Zero,
          child.members,
          parentProject.version,
          parentProject.activities,
          parentProject.datasets,
          parentProject
        )
      )

    def importDataset[PIN <: Dataset.Provenance, POUT <: Dataset.Provenance](
        dataset:              Dataset[PIN]
    )(implicit newProvenance: ProvenanceImportFactory[PIN, POUT]): (Dataset[POUT], Project) = {
      val newIdentifier = datasetIdentifiers.generateOne
      val importedDS = dataset.copy(
        identification = dataset.identification.copy(identifier = newIdentifier),
        provenance = newProvenance(
          Dataset.entityId(newIdentifier),
          SameAs(dataset.entityId),
          dataset.provenance,
          InitialVersion(newIdentifier)
        )
      )
      importedDS -> (project addDatasets importedDS)
    }
  }

  implicit class DatasetOps[P <: Dataset.Provenance](dataset: Dataset[P])(implicit renkuBaseUrl: RenkuBaseUrl) {

    lazy val identifier: datasets.Identifier = dataset.identification.identifier

    def to[T](implicit convert: Dataset[P] => T): T = convert(dataset)

    def widen[T <: Dataset.Provenance](implicit ev: P <:< T): Dataset[T] = dataset.asInstanceOf[Dataset[T]]

    def invalidate(time: InvalidationTime): ValidatedNel[String, Dataset[Dataset.Provenance.Modified]] = {
      val newIdentifier = datasetIdentifiers.generateOne
      dataset.provenance.date match {
        case dateCreated: DateCreated =>
          Validated.condNel(
            test = (time.value compareTo dateCreated.instant) >= 0,
            dataset.copy(
              identification = dataset.identification.copy(identifier = newIdentifier),
              provenance = Dataset.Provenance.Modified(
                Dataset.entityId(newIdentifier),
                DerivedFrom(dataset.entityId),
                dataset.provenance.topmostDerivedFrom,
                dataset.provenance.initialVersion,
                datasets.DateCreated(time.value),
                dataset.provenance.creators + personEntities.generateOne,
                maybeInvalidationTime = time.some
              )
            ),
            s"Invalidation time $time on dataset with id: ${dataset.identification.identifier} is older than dataset date"
          )
        case _ =>
          dataset
            .copy(
              identification = dataset.identification.copy(identifier = newIdentifier),
              provenance = Dataset.Provenance.Modified(
                Dataset.entityId(newIdentifier),
                DerivedFrom(dataset.entityId),
                dataset.provenance.topmostDerivedFrom,
                dataset.provenance.initialVersion,
                datasets.DateCreated(time.value),
                dataset.provenance.creators + personEntities.generateOne,
                maybeInvalidationTime = time.some
              )
            )
            .validNel
      }
    }

    def invalidateNow: Dataset[Dataset.Provenance.Modified] = invalidateUnsafe(InvalidationTime.now)

    def invalidateUnsafe(time: InvalidationTime): Dataset[Dataset.Provenance.Modified] =
      invalidate(time).fold(errors => throw new IllegalArgumentException(errors.intercalate(", ")), identity)

    def invalidatePartNow(part: DatasetPart): Dataset[Provenance.Modified] =
      invalidatePart(part, InvalidationTime.now)
        .fold(errors => throw new IllegalArgumentException(errors.intercalate("; ")), identity)

    def invalidatePart(part: DatasetPart,
                       time: InvalidationTime
    ): ValidatedNel[String, Dataset[Dataset.Provenance.Modified]] =
      dataset.parts
        .find(_ == part)
        .toValidNel(s"There's no part ${part.entity.location} on dataset with id ${dataset.identifier}")
        .andThen(_.invalidate(time))
        .map { invalidatedPart =>
          val newIdentifier = datasetIdentifiers.generateOne
          dataset.copy(
            dataset.identification.copy(identifier = newIdentifier),
            provenance = Dataset.Provenance.Modified(
              Dataset.entityId(newIdentifier),
              DerivedFrom(dataset.entityId),
              dataset.provenance.topmostDerivedFrom,
              dataset.provenance.initialVersion,
              datasets.DateCreated(time.value),
              dataset.provenance.creators + personEntities.generateOne,
              maybeInvalidationTime = None
            ),
            parts = dataset.parts.filterNot(_ == part) ::: invalidatedPart :: Nil
          )
        }

    def createModification(
        modifier: Dataset[Dataset.Provenance.Modified] => Dataset[Dataset.Provenance.Modified] = identity
    ): DatasetGenFactory[Provenance.Modified] =
      (projectDate => modifiedDatasetEntities(dataset, projectDate)).modify(modifier)

    def makeNameContaining(phrase: String): Dataset[P] = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase)
      dataset.copy(
        identification = dataset.identification.copy(name =
          sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
        )
      )
    }

    def makeTitleContaining(phrase: String): Dataset[P] = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase)
      dataset.copy(
        identification = dataset.identification.copy(title =
          sentenceContaining(nonEmptyPhrase).map(_.value).map(Title.apply).generateOne
        )
      )
    }

    def makeCreatorNameContaining(phrase: String)(implicit provenanceUpdater: (users.Name, P) => P): Dataset[P] = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase)
      dataset.copy(
        provenance =
          provenanceUpdater(sentenceContaining(nonEmptyPhrase).map(_.value).map(users.Name.apply).generateOne,
                            dataset.provenance
          )
      )
    }

    def makeKeywordsContaining(phrase: String): Dataset[P] =
      dataset.copy(
        additionalInfo = dataset.additionalInfo.copy(keywords = dataset.additionalInfo.keywords :+ Keyword(phrase))
      )

    def makeDescContaining(phrase: String): Dataset[P] =
      dataset.copy(additionalInfo =
        dataset.additionalInfo.copy(maybeDescription =
          sentenceContaining(Refined.unsafeApply(phrase)).map(_.value).map(Description.apply).generateSome
        )
      )
  }

  type ProvenanceImportFactory[OldProvenance <: Dataset.Provenance, NewProvenance <: Dataset.Provenance] =
    (EntityId, InternalSameAs, OldProvenance, InitialVersion) => NewProvenance

  lazy val importedInternal: ProvenanceImportFactory[Dataset.Provenance, Dataset.Provenance.ImportedInternal] = {
    case (newEntityId, sameAs, oldProvenance: Dataset.Provenance.Internal, initialVersion) =>
      fromInternalToImportedInternalAncestorInternal(newEntityId, sameAs, oldProvenance, initialVersion)
    case (newEntityId, sameAs, oldProvenance: Dataset.Provenance.ImportedExternal, initialVersion) =>
      fromImportedExternalToImportedInternalAncestorExternal(newEntityId, sameAs, oldProvenance, initialVersion)
    case (newEntityId, sameAs, oldProvenance: Dataset.Provenance.ImportedInternalAncestorInternal, initialVersion) =>
      fromImportedInternalAncestorInternalToImportedInternalAncestorInternal(newEntityId,
                                                                             sameAs,
                                                                             oldProvenance,
                                                                             initialVersion
      )
    case (newEntityId, sameAs, oldProvenance: Dataset.Provenance.ImportedInternalAncestorExternal, initialVersion) =>
      fromImportedInternalAncestorExternalToImportedInternalAncestorExternal(newEntityId,
                                                                             sameAs,
                                                                             oldProvenance,
                                                                             initialVersion
      )
    case (newEntityId, sameAs, oldProvenance: Dataset.Provenance.Modified, initialVersion) =>
      importFromModifiedToImportedInternalAncestorInternal(newEntityId, sameAs, oldProvenance, initialVersion)
  }

  implicit lazy val fromInternalToImportedInternalAncestorInternal
      : ProvenanceImportFactory[Dataset.Provenance.Internal, Dataset.Provenance.ImportedInternalAncestorInternal] =
    (newEntityId, sameAs, oldProvenance, initialVersion) =>
      Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                          sameAs,
                                                          oldProvenance.topmostSameAs,
                                                          initialVersion,
                                                          oldProvenance.date,
                                                          oldProvenance.creators
      )

  implicit lazy val fromImportedExternalToImportedInternalAncestorExternal
      : ProvenanceImportFactory[Dataset.Provenance.ImportedExternal,
                                Dataset.Provenance.ImportedInternalAncestorExternal
      ] = (newEntityId, sameAs, oldProvenance, initialVersion) =>
    Dataset.Provenance.ImportedInternalAncestorExternal(newEntityId,
                                                        sameAs,
                                                        oldProvenance.topmostSameAs,
                                                        initialVersion,
                                                        oldProvenance.date,
                                                        oldProvenance.creators
    )

  implicit lazy val fromImportedInternalAncestorExternalToImportedInternalAncestorExternal
      : ProvenanceImportFactory[Dataset.Provenance.ImportedInternalAncestorExternal,
                                Dataset.Provenance.ImportedInternalAncestorExternal
      ] = (newEntityId, sameAs, oldProvenance, initialVersion) =>
    Dataset.Provenance.ImportedInternalAncestorExternal(newEntityId,
                                                        sameAs,
                                                        oldProvenance.topmostSameAs,
                                                        initialVersion,
                                                        oldProvenance.date,
                                                        oldProvenance.creators
    )

  implicit lazy val fromImportedInternalAncestorInternalToImportedInternalAncestorInternal
      : ProvenanceImportFactory[Dataset.Provenance.ImportedInternalAncestorInternal,
                                Dataset.Provenance.ImportedInternalAncestorInternal
      ] = (newEntityId, sameAs, oldProvenance, initialVersion) =>
    Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                        sameAs,
                                                        oldProvenance.topmostSameAs,
                                                        initialVersion,
                                                        oldProvenance.date,
                                                        oldProvenance.creators
    )

  implicit lazy val importFromModifiedToImportedInternalAncestorInternal
      : ProvenanceImportFactory[Dataset.Provenance.Modified, Dataset.Provenance.ImportedInternalAncestorInternal] =
    (newEntityId, sameAs, oldProvenance, initialVersion) =>
      Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                          sameAs,
                                                          oldProvenance.topmostSameAs,
                                                          initialVersion,
                                                          oldProvenance.date,
                                                          oldProvenance.creators
      )

  implicit val creatorUsernameUpdaterInternal
      : (users.Name, Dataset.Provenance.Internal) => Dataset.Provenance.Internal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedInternalAncestorInternal
      : (users.Name,
         Dataset.Provenance.ImportedInternalAncestorInternal
      ) => Dataset.Provenance.ImportedInternalAncestorInternal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedInternalAncestorExternal
      : (users.Name,
         Dataset.Provenance.ImportedInternalAncestorExternal
      ) => Dataset.Provenance.ImportedInternalAncestorExternal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedExternal
      : (users.Name, Dataset.Provenance.ImportedExternal) => Dataset.Provenance.ImportedExternal = {
    case (userName, prov) =>
      prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterModified
      : (users.Name, Dataset.Provenance.Modified) => Dataset.Provenance.Modified = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit class DatasetPartOps(part: DatasetPart) {

    def to[T](implicit convert: DatasetPart => T): T = convert(part)

    private[ModelOps] def invalidate(
        time: InvalidationTime
    ): ValidatedNel[String, DatasetPart with HavingInvalidationTime] =
      Validated.condNel(
        test = (time.value compareTo part.dateCreated.value) >= 0,
        new DatasetPart(datasets.PartId.generate, part.external, part.entity, part.dateCreated, part.maybeSource)
          with HavingInvalidationTime {
          override val invalidationTime: InvalidationTime = time
        },
        s"Invalidation time $time is older than dataset part ${part.entity.location}"
      )
  }

  implicit class PlanOps(plan: Plan) {

    def to[T](implicit convert: Plan => T): T = convert(plan)

    def invalidate(time: InvalidationTime): Plan with HavingInvalidationTime =
      new Plan(plan.id,
               plan.name,
               plan.maybeDescription,
               plan.command,
               plan.maybeProgrammingLanguage,
               plan.keywords,
               plan.commandParameterFactories,
               plan.successCodes
      ) with HavingInvalidationTime {
        override val invalidationTime: InvalidationTime = time
      }
  }

  implicit class CommandParameterBaseOps[P <: CommandParameterBase](parameter: P) {
    def to[T](implicit convert: P => T): T = convert(parameter)
  }

  implicit class AssociationOps(association: Association) {
    def to[T](implicit convert: Association => T): T = convert(association)
  }

  implicit class AgentOps(agent: Agent) {
    def to[T](implicit convert: Agent => T): T = convert(agent)
  }

  implicit class EntityOps[E <: Entity](entity: E) {
    def to[T](implicit convert: E => T): T = convert(entity)
  }

  implicit class UsageOps(usage: Usage) {
    def to[T](implicit convert: Usage => T): T = convert(usage)
  }

  implicit class GenerationOps(generation: Generation) {
    def to[T](implicit convert: Generation => T): T = convert(generation)
  }

  implicit class ParameterValueOps[P <: ParameterValue](parameter: P) {
    def to[T](implicit convert: P => T): T = convert(parameter)
  }

  implicit class ActivityOps(activity: Activity) {
    def to[T](implicit convert: Activity => T): T = convert(activity)
  }

  implicit class PublicationEventOps(publicationEvent: PublicationEvent) {
    def to[T](implicit convert: PublicationEvent => T): T = convert(publicationEvent)
  }

  implicit class ProvenanceOps[P <: Dataset.Provenance](provenance: P) {
    def to[T](implicit convert: P => T): T = convert(provenance)
  }
}

object ModelOps extends ModelOps {
  final case class DatasetForkingResult[DP <: Dataset.Provenance](original: Dataset[DP], fork: Dataset[DP])
}
