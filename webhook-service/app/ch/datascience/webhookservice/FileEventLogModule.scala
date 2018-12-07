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

import ch.datascience.webhookservice.queues.commitevent.{ EventLogSinkProvider, FileEventLogSinkProvider }
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import com.typesafe.config.Config
import play.api.{ ConfigLoader, Configuration, Environment }

class FileEventLogModule( environment: Environment, configuration: Configuration )
  extends AbstractModule {

  import FileEventLogModule.{ FileEventLogDisabled, FileEventLogEnabled }

  private val module: AbstractModule = configuration.getOptional[Boolean]( "file-event-log.enabled" ) match {
    case Some( true ) => new FileEventLogEnabled( configuration )
    case _            => FileEventLogDisabled
  }

  override def configure(): Unit =
    module.configure( binder() )
}

private object FileEventLogModule {

  import java.nio.file._

  private implicit object PathConfigReader extends ConfigLoader[Path] {
    override def load( config: Config, path: String ): Path =
      FileSystems.getDefault.getPath( config.getString( path ) )
  }

  private case class FileEventLogEnabled(
      filePath:     Path,
      deleteOnExit: Boolean
  ) extends AbstractModule {

    def this( configuration: Configuration ) = this(
      configuration.get[Path]( "file-event-log.file-path" ),
      configuration.get[Boolean]( "file-event-log.delete-on-exit" )
    )

    override def configure(): Unit = {

      bind( classOf[Path] )
        .annotatedWith( Names.named( "event-log-file-path" ) )
        .toInstance {
          val validatedPath =
            if ( !Files.exists( filePath ) ) Files.createFile( filePath )
            else filePath
          if ( deleteOnExit ) validatedPath.toFile.deleteOnExit()
          validatedPath
        }

      bind( classOf[EventLogSinkProvider] )
        .to( classOf[FileEventLogSinkProvider] )
    }
  }

  private case object FileEventLogDisabled extends AbstractModule {
    override def configure(): Unit = {}
  }
}
