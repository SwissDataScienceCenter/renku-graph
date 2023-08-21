{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "webhookService.name" -}}
{{- "webhook-service" -}}
{{- end -}}

{{- define "triplesGenerator.name" -}}
{{- "triples-generator" -}}
{{- end -}}

{{- define "tokenRepository.name" -}}
{{- "token-repository" -}}
{{- end -}}

{{- define "knowledgeGraph.name" -}}
{{- "knowledge-graph" -}}
{{- end -}}

{{- define "eventLog.name" -}}
{{- "event-log" -}}
{{- end -}}

{{- define "commitEventService.name" -}}
{{- "commit-event-service" -}}
{{- end -}}

{{- define "jena.name" -}}
{{- "jena" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "webhookService.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-webhook-service" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "triplesGenerator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-triples-generator" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "tokenRepository.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-token-repository" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "knowledgeGraph.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-knowledge-graph" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "eventLog.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-event-log" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "jena.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-jena" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "commitEventService.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-commit-event-service" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "renkuCoreLatest.fullname" -}}
{{- $coreBaseName := printf "%s-core" .Release.Name -}}
{{- printf "%s-%s" $coreBaseName (get $.Values.global.core.versions "latest").name -}}
{{- end -}}

{{/*
Comma separated list of renku-core service names
*/}}
{{- define "renkuCore.serviceUrls" -}}
{{- $serviceUrls := list -}}
{{- $coreBaseName := printf "%s-core" .Release.Name -}}
{{- range $i, $k := (keys .Values.global.core.versions | sortAlpha) -}}
{{- $serviceUrl := printf "http://%s-%s" $coreBaseName (get $.Values.global.core.versions $k).name -}}
{{- $serviceUrls = mustAppend $serviceUrls $serviceUrl -}}
{{- end -}}
{{- join "," $serviceUrls | quote -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "graph.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Define http scheme
*/}}
{{- define "http" -}}
{{- if .Values.global.useHTTPS -}}
https
{{- else -}}
http
{{- end -}}
{{- end -}}
