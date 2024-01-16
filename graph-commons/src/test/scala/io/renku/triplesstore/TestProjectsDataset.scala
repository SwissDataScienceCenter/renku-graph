/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore

import TestDatasetCreation._
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.renku.graph.model._
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.triplesstore.DatasetTTLs.ProjectsTTL
import io.renku.http.client.GitLabApiUrl
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntityLike}
import io.renku.jsonld.{EntityId, Graph, JsonLDEncoder}
import io.renku.triplesstore.client.http.SparqlClient
import org.typelevel.log4cats.Logger

trait TestProjectsDataset extends TestDataset {
  self: GraphJenaSpec =>

  def uploadToProjects(graphs: Graph*)(implicit pcc: ProjectsConnectionConfig, L: Logger[IO]): IO[Unit] =
    SparqlClient[IO](pcc.toCC()).use { client =>
      graphs.toList
        .traverse_(g => IO.fromEither(g.flatten) >>= client.upload)
    }

  def uploadToProjects[T](objects: T*)(implicit
      entityFunctions: EntityFunctions[T],
      graphsProducer:  GraphsProducer[T],
      pcc:             ProjectsConnectionConfig,
      L:               Logger[IO]
  ): IO[Unit] =
    uploadToProjects(objects >>= graphsProducer.apply: _*)

  def projectsDSConfig(implicit L: Logger[IO]): Resource[IO, ProjectsConnectionConfig] =
    ttlResource >>= { ttl =>
      (clientResource >>= datasetResource(ttl))
        .as(dsConnectionConfig(ttl, server.conConfig, ProjectsConnectionConfig.apply))
    }

  def projectsDSResource(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    projectsDSConfig
      .flatMap(pcc => SparqlClient[IO](pcc.toCC()))

  private lazy val ttlResource =
    loadTtl(ProjectsTTL)
      .flatMap(ttl => generateName(ttl, getClass).tupleLeft(ttl))
      .map { case (origTtl, newName) => updateDSConfig(origTtl, newName, new ProjectsTTL(_, _)) }
      .toResource

  trait GraphsProducer[T] {
    def apply(obj: T)(implicit entityFunctions: EntityFunctions[T]): List[Graph]
  }

  protected implicit def projectsDSGraphsProducer[A](implicit
      renkuUrl: RenkuUrl,
      glApiUrl: GitLabApiUrl
  ): GraphsProducer[A] = new GraphsProducer[A] {
    import io.renku.graph.model.entities
    import io.renku.jsonld.NamedGraph
    import io.renku.jsonld.syntax._

    override def apply(entity: A)(implicit entityFunctions: EntityFunctions[A]): List[Graph] =
      List(maybeBuildProjectGraph(entity), maybeBuildPersonsGraph(entity)).flatten

    private def maybeBuildProjectGraph(entity: A)(implicit entityFunctions: EntityFunctions[A]) = {
      implicit val projectEnc: JsonLDEncoder[A] = entityFunctions.encoder(GraphClass.Project)
      entity.asJsonLD match {
        case jsonLD: JsonLDEntityLike => NamedGraph(projectGraphId(entity), jsonLD).some
        case jsonLD: JsonLDArray      => NamedGraph.fromJsonLDsUnsafe(projectGraphId(entity), jsonLD).some
        case _ => None
      }
    }

    private def maybeBuildPersonsGraph(entity: A)(implicit entityFunctions: EntityFunctions[A]) = {
      implicit val graph: GraphClass = GraphClass.Persons
      entityFunctions.findAllPersons(entity).toList.map(_.asJsonLD) match {
        case Nil    => None
        case h :: t => NamedGraph.fromJsonLDsUnsafe(GraphClass.Persons.id, h, t: _*).some
      }
    }

    private def projectGraphId(entity: A): EntityId = entity match {
      case p: entities.Project     => GraphClass.Project.id(p.resourceId)
      case p: testentities.Project => GraphClass.Project.id(projects.ResourceId(p.asEntityId))
      case _ => defaultProjectGraphId
    }
  }
}
