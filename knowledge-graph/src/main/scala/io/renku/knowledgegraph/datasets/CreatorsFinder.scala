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

package io.renku.knowledgegraph.datasets

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.Async
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets._
import io.renku.graph.model.persons.{Affiliation, Email, Name => UserName}
import io.renku.graph.model.{GraphClass, projects}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait CreatorsFinder[F[_]] {
  def findCreators(identifier: Identifier, projectId: projects.ResourceId): F[NonEmptyList[DatasetCreator]]
}

private class CreatorsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClient(storeConfig)
    with CreatorsFinder[F] {

  import CreatorsFinder._

  def findCreators(identifier: Identifier, projectId: projects.ResourceId): F[NonEmptyList[DatasetCreator]] = {
    implicit val decoder: Decoder[NonEmptyList[DatasetCreator]] = creatorsDecoder(identifier)
    queryExpecting[NonEmptyList[DatasetCreator]](query(identifier, projectId))
  }

  private def query(identifier: Identifier, projectId: projects.ResourceId) = SparqlQuery.of(
    name = "ds by id - creators",
    Prefixes of schema -> "schema",
    s"""|SELECT DISTINCT ?email ?name ?affiliation
        |FROM <${GraphClass.Project.id(projectId)}> 
        |FROM <${GraphClass.Persons.id}> {
        |  ?dataset a schema:Dataset ;
        |           schema:identifier '$identifier';
        |           schema:creator ?creatorResource.
        |  
        |  OPTIONAL { 
        |    ?creatorResource schema:email ?email
        |  }
        |  OPTIONAL { 
        |    ?creatorResource schema:affiliation ?affiliation
        |  }
        |  ?creatorResource a schema:Person;
        |                   schema:name ?name
        |}
        |""".stripMargin
  )
}

private object CreatorsFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig): F[CreatorsFinder[F]] =
    MonadThrow[F].catchNonFatal(new CreatorsFinderImpl(storeConfig))

  import ResultsDecoder._
  import io.circe.Decoder

  private[datasets] def creatorsDecoder(identifier: Identifier): Decoder[NonEmptyList[DatasetCreator]] =
    ResultsDecoder[NonEmptyList, DatasetCreator] { implicit cursor =>
      import io.renku.tinytypes.json.TinyTypeDecoders._

      for {
        maybeEmail       <- extract[Option[Email]]("email")
        name             <- extract[UserName]("name")
        maybeAffiliation <- extract[Option[Affiliation]]("affiliation")
      } yield DatasetCreator(maybeEmail, name, maybeAffiliation)
    }(toNonEmptyList(onEmpty = s"No creators on dataset $identifier")).map(_.sortBy(_.name))
}
