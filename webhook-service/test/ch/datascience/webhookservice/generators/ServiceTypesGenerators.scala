/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.generators

import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.webhookservice.config.DatasetType.{ Mem, TDB }
import ch.datascience.webhookservice.config._
import ch.datascience.webhookservice.queues.pushevent._
import org.apache.jena.rdf.model.ModelFactory
import org.scalacheck.Gen

object ServiceTypesGenerators {

  implicit val pushEvents: Gen[PushEvent] = for {
    before <- commitIds
    after <- commitIds
    pushUser <- pushUsers
    project <- projects
  } yield PushEvent( before, after, pushUser, project )

  implicit val rdfTriplesSets: Gen[RDFTriples] = for {
    model <- Gen.uuid.map( _ => ModelFactory.createDefaultModel() )
    subject <- nonEmptyStrings() map model.createResource
    predicate <- nonEmptyStrings() map model.createProperty
  } yield {
    val `object` = model.createResource
    model.add( subject, predicate, `object` )
    RDFTriples( model )
  }

  implicit val serviceUrls: Gen[ServiceUrl] = httpUrls map ServiceUrl.apply

  implicit val fusekiConfigs: Gen[FusekiConfig] = for {
    fusekiUrl <- serviceUrls
    datasetName <- nonEmptyStrings() map DatasetName.apply
    datasetType <- Gen.oneOf( Mem, TDB )
    username <- nonEmptyStrings() map Username.apply
    password <- nonEmptyStrings() map Password.apply
  } yield FusekiConfig( fusekiUrl, datasetName, datasetType, username, password )
}
