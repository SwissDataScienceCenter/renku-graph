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

package io.renku.cli.model

import io.renku.graph.model.Schemas.{prov, renku, schema}

//noinspection TypeAnnotation
object Ontologies {

  object Schema {
    val Action           = schema / "Action"
    val CreativeWork     = schema / "CreativeWork"
    val Dataset          = schema / "Dataset"
    val DigitalDocument  = schema / "DigitalDocument"
    val Person           = schema / "Person"
    val Project          = schema / "Project"
    val PropertyValue    = schema / "PropertyValue"
    val PublicationEvent = schema / "PublicationEvent"
    val URL              = schema / "URL"

    val about          = schema / "about"
    val affiliation    = schema / "affiliation"
    val agent          = schema / "agent"
    val creator        = schema / "creator"
    val dateCreated    = schema / "dateCreated"
    val dateModified   = schema / "dateModified"
    val datePublished  = schema / "datePublished"
    val defaultValue   = schema / "defaultValue"
    val description    = schema / "description"
    val email          = schema / "email"
    val encodingFormat = schema / "encodingFormat"
    val hasPart        = schema / "hasPart"
    val identifier     = schema / "identifier"
    val image          = schema / "image"
    val keywords       = schema / "keywords"
    val license        = schema / "license"
    val name           = schema / "name"
    val sameAs         = schema / "sameAs"
    val startDate      = schema / "startDate"
    val schemaVersion  = schema / "schemaVersion"
    val url            = schema / "url"
    val value          = schema / "value"
    val valueReference = schema / "valueReference"
    val version        = schema / "version"
  }

  object Prov {
    val Activity      = prov / "Activity"
    val Association   = prov / "Association"
    val Collection    = prov / "Collection"
    val Entity        = prov / "Entity"
    val Generation    = prov / "Generation"
    val Location      = prov / "Location"
    val Person        = prov / "Person"
    val Plan          = prov / "Plan"
    val SoftwareAgent = prov / "SoftwareAgent"
    val Usage         = prov / "Usage"

    val activity             = prov / "activity"
    val atLocation           = prov / "atLocation"
    val agent                = prov / "agent"
    val endedAtTime          = prov / "endedAtTime"
    val entity               = prov / "entity"
    val hadPlan              = prov / "hadPlan"
    val invalidatedAtTime    = prov / "invalidatedAtTime"
    val qualifiedAssociation = prov / "qualifiedAssociation"
    val qualifiedGeneration  = prov / "qualifiedGeneration"
    val qualifiedUsage       = prov / "qualifiedUsage"
    val startedAtTime        = prov / "startedAtTime"
    val wasAssociatedWith    = prov / "wasAssociatedWith"
    val wasDerivedFrom       = prov / "wasDerivedFrom"
  }

  object Renku {
    val CommandInput              = renku / "CommandInput"
    val CommandOutput             = renku / "CommandOutput"
    val CommandParameter          = renku / "CommandParameter"
    val CommandParameterBase      = renku / "CommandParameterBase"
    val CompositePlan             = renku / "CompositePlan"
    val IOStream                  = renku / "IOStream"
    val ParameterLink             = renku / "ParameterLink"
    val ParameterMapping          = renku / "ParameterMapping"
    val ParameterValue            = renku / "ParameterValue"
    val Plan                      = renku / "Plan"
    val WorkflowFileCompositePlan = renku / "WorkflowFileCompositePlan"
    val WorkflowFilePlan          = renku / "WorkflowFilePlan"

    val checksum           = renku / "checksum"
    val command            = renku / "command"
    val createFolder       = renku / "createFolder"
    val external           = renku / "external"
    val hasActivity        = renku / "hasActivity"
    val hasArguments       = renku / "hasArguments"
    val hasDataset         = renku / "hasDataset"
    val hasInputs          = renku / "hasInputs"
    val hasMappings        = renku / "hasMappings"
    val hasOutputs         = renku / "hasOutputs"
    val hasPlan            = renku / "hasPlan"
    val hasSubprocess      = renku / "hasSubprocess"
    val linkSource         = renku / "linkSource"
    val linkSink           = renku / "linkSink"
    val mappedTo           = renku / "mappedTo"
    val mapsTo             = renku / "mapsTo"
    val originalIdentifier = renku / "originalIdentifier"
    val parameter          = renku / "parameter"
    val position           = renku / "position"
    val prefix             = renku / "prefix"
    val slug               = renku / "slug"
    val source             = renku / "source"
    val streamType         = renku / "streamType"
    val successCodes       = renku / "successCodes"
    val workflowLink       = renku / "workflowLinks"
  }
}
