/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator

import java.net.URL

import ch.datascience.config.ServiceUrl
import ch.datascience.triplesgenerator.queues.logevent.LogEventQueue
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import play.api.{ Configuration, Environment }

class ServiceModule(
    environment:   Environment,
    configuration: Configuration
) extends AbstractModule {

  override def configure(): Unit = {

    bind( classOf[FusekiDatasetVerifier] )
      .asEagerSingleton()

    bind( classOf[URL] )
      .annotatedWith( Names.named( "gitlabUrl" ) )
      .toInstance( configuration.get[ServiceUrl]( "services.gitlab.url" ).value )

    bind( classOf[LogEventQueue] )
      .asEagerSingleton()
  }
}
