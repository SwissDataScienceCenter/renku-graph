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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.graph.triplesstore.DatasetTTLs.ProjectsTTL
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import reprovisioning.{ReProvisioning, ReProvisioningStatus}

private[tsmigrationrequest] object Migrations {

  def apply[F[_]: Async: ReProvisioningStatus: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      config: Config
  ): F[List[Migration[F]]] = for {
    datasetsCreator                     <- DatasetsCreator[F]
    datasetsRemover                     <- DatasetsRemover[F]
    reProvisioning                      <- ReProvisioning[F](config)
    removeNotLinkedPersons              <- RemoveNotLinkedPersons[F]
    fixMultipleProjectCreatedDates      <- FixMultipleProjectCreatedDates[F]
    addRenkuPlanWhereMissing            <- AddRenkuPlanWhereMissing[F]
    migrationToV10                      <- v10migration.MigrationToV10[F]
    v10VersionSetter                    <- V10VersionUpdater[F]
    projectsDateViewedCreator           <- ProjectsDateViewedCreator[F]
    projectDateViewedDeduplicator       <- ProjectDateViewedDeduplicator[F]
    personViewedEntityDeduplicator      <- PersonViewedEntityDeduplicator[F]
    provisionProjectsGraph              <- projectsgraph.ProvisionProjectsGraph[F]
    addProjectDateModified              <- datemodified.AddProjectDateModified[F]
    fixMultipleProjectVersions          <- FixMultipleProjectVersions[F]
    addProjectSlug                      <- projectslug.AddProjectSlug[F]
    datasetsGraphPersonRemover          <- DatasetsGraphPersonRemover[F]
    projectsGraphPersonRemover          <- ProjectsGraphPersonRemover[F]
    reindexLuceneStdTokenizer           <- lucenereindex.ReindexLucene[F](suffix = "- std tokenizer")
    projectsDSRecreatorSearchFlattening <- TSDatasetRecreator[F, ProjectsTTL]("- search flattening", ProjectsTTL)
    projectsGraphKeywordsFlattener      <- ProjectsGraphKeywordsFlattener[F]
    projectsGraphImagesFlattener        <- ProjectsGraphImagesFlattener[F]
    datasetsGraphKeywordsFlattener      <- DatasetsGraphKeywordsFlattener[F]
    datasetsGraphImagesFlattener        <- DatasetsGraphImagesFlattener[F]
    datasetsGraphCreatorsFlattener      <- DatasetsGraphCreatorsFlattener[F]
    datasetsGraphSlugsVisibsFlattener   <- DatasetsGraphSlugsVisibilitiesFlattener[F]
    migrations <- validateNames(
                    datasetsCreator,
                    datasetsRemover,
                    reProvisioning,
                    removeNotLinkedPersons,
                    fixMultipleProjectCreatedDates,
                    addRenkuPlanWhereMissing,
                    migrationToV10,
                    v10VersionSetter,
                    projectsDateViewedCreator,
                    projectDateViewedDeduplicator,
                    personViewedEntityDeduplicator,
                    provisionProjectsGraph,
                    addProjectDateModified,
                    fixMultipleProjectVersions,
                    addProjectSlug,
                    datasetsGraphPersonRemover,
                    projectsGraphPersonRemover,
                    reindexLuceneStdTokenizer,
                    projectsDSRecreatorSearchFlattening,
                    projectsGraphKeywordsFlattener,
                    projectsGraphImagesFlattener,
                    datasetsGraphKeywordsFlattener,
                    datasetsGraphImagesFlattener,
                    datasetsGraphCreatorsFlattener,
                    datasetsGraphSlugsVisibsFlattener
                  )
  } yield migrations

  private[migrations] def validateNames[F[_]: MonadThrow: Logger](migrations: Migration[F]*): F[List[Migration[F]]] = {
    val groupedByName = migrations.groupBy(_.name)
    val problematicMigrations = groupedByName.collect {
      case (name, ms) if ms.size > 1 => name
    }
    if (problematicMigrations.nonEmpty) {
      val error = show"$categoryName: there are multiple migrations with name: ${problematicMigrations.mkString("; ")}"
      Logger[F]
        .error(error) >> new Exception(error).raiseError[F, List[Migration[F]]].map(_ => List.empty[Migration[F]])
    } else migrations.toList.pure[F]
  }
}
