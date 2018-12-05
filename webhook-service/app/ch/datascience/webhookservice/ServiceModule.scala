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

package ch.datascience.webhookservice

import java.net.URL
import java.nio.file.Path

import ch.datascience.config.ServiceUrl
import ch.datascience.webhookservice.queues.commitevent.{ EventLogSinkProvider, FileEventLogSinkProvider }
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import play.api.{ Configuration, Environment }

class ServiceModule(
    environment:   Environment,
    configuration: Configuration
) extends AbstractModule {

  override def configure(): Unit = {

    bind( classOf[URL] )
      .annotatedWith( Names.named( "gitlabUrl" ) )
      .toInstance( configuration.get[ServiceUrl]( "services.gitlab.url" ).value )

    bind( classOf[Path] )
      .annotatedWith( Names.named( "event-log-file-path" ) )
      .toInstance {
        import java.nio.file._
        val path = FileSystems.getDefault.getPath( "/tmp/renku-event.log" )
        val validatedPath =
          if ( !Files.exists( path ) ) Files.createFile( path )
          else path
        validatedPath.toFile.deleteOnExit()
        path
      }

    bind( classOf[EventLogSinkProvider] )
      .to( classOf[FileEventLogSinkProvider] )
  }
}
