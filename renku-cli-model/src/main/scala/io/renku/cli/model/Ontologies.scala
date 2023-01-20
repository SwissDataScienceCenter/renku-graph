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
private[cli] object Ontologies {

  object Schema {
    val Action          = schema / "Action"
    val CreativeWork    = schema / "CreativeWork"
    val Dataset         = schema / "Dataset"
    val DigitalDocument = schema / "DigitalDocument"
    val URL             = schema / "URL"
    val PropertyValue   = schema / "PropertyValue"
    val Person          = schema / "Person"
    val Project         = schema / "Project"

    val affiliation    = schema / "affiliation"
    val agent          = schema / "agent"
    val email          = schema / "email"
    val valueReference = schema / "valueReference"
    val value          = schema / "value"
    val name           = schema / "name"
    val description    = schema / "description"
    val creator        = schema / "creator"
    val dateCreated    = schema / "dateCreated"
    val dateModified   = schema / "dateModified"
    val keywords       = schema / "keywords"
    val defaultValue   = schema / "defaultValue"
    val encodingFormat = schema / "encodingFormat"
    val identifier     = schema / "identifier"
    val datePublished  = schema / "datePublished"
    val sameAs         = schema / "sameAs"
    val schemaVersion  = schema / "schemaVersion"
    val image          = schema / "image"
    val license        = schema / "license"
    val version        = schema / "version"
    val hasPart        = schema / "hasPart"
    val hasPlan        = schema / "hasPlan"
    val hasActivity    = schema / "hasActivity"
    val hasDataset     = schema / "hasDataset"
    val url            = schema / "url"
  }

  object Prov {
    val Activity      = prov / "Activity"
    val Entity        = prov / "Entity"
    val Plan          = prov / "Plan"
    val SoftwareAgent = prov / "SoftwareAgent"
    val Collection    = prov / "Collection"
    val Person        = prov / "Person"
    val Association   = prov / "Association"
    val Usage         = prov / "Usage"
    val Generation    = prov / "Generation"
    val Location      = prov / "Location"

    val hadMember            = prov / "hadMember"
    val activity             = prov / "activity"
    val agent                = prov / "agent"
    val hadPlan              = prov / "hadPlan"
    val wasDerivedFrom       = prov / "wasDerivedFrom"
    val invalidatedAtTime    = prov / "invalidatedAtTime"
    val entity               = prov / "entity"
    val atLocation           = prov / "atLocation"
    val qualifiedGeneration  = prov / "qualifiedGeneration"
    val qualifiedAssociation = prov / "qualifiedAssociation"
    val endedAtTime          = prov / "endedAtTime"
    val startedAtTime        = prov / "startedAtTime"
    val wasAssociatedWith    = prov / "wasAssociatedWith"
    val qualifiedUsage       = prov / "qualifiedUsage"
  }

  object Renku {
    val Plan                      = renku / "Plan"
    val CompositePlan             = renku / "CompositePlan"
    val CommandOutput             = renku / "CommandOutput"
    val CommandParameterBase      = renku / "CommandParameterBase"
    val CommandInput              = renku / "CommandInput"
    val CommandParameter          = renku / "CommandParameter"
    val IOStream                  = renku / "IOStream"
    val ParameterLink             = renku / "ParameterLink"
    val ParameterMapping          = renku / "ParameterMapping"
    val ParameterValue            = renku / "ParameterValue"
    val WorkflowFilePlan          = renku / "WorkflowFilePlan"
    val WorkflowFileCompositePlan = renku / "WorkflowFileCompositePlan"

    val command            = renku / "command"
    val linkSource         = renku / "linkSource"
    val linkSink           = renku / "linkSink"
    val streamType         = renku / "streamType"
    val checksum           = renku / "checksum"
    val workflowLink       = renku / "workflowLinks"
    val hasMappings        = renku / "hasMappings"
    val hasSubprocess      = renku / "hasSubprocess"
    val hasArguments       = renku / "hasArguments"
    val hasInputs          = renku / "hasInputs"
    val hasOutputs         = renku / "hasOutputs"
    val successCodes       = renku / "successCodes"
    val position           = renku / "position"
    val prefix             = renku / "prefix"
    val mappedTo           = renku / "mappedTo"
    val mapsTo             = renku / "mapsTo"
    val createFolder       = renku / "createFolder"
    val slug               = renku / "slug"
    val originalIdentifier = renku / "originalIdentifier"
    val external           = renku / "external"
    val source             = renku / "source"
    val parameter          = renku / "parameter"
  }
}
