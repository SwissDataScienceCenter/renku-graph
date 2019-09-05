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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "webhookService.chart" -}}
{{- printf "%s-webhook-service" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "triplesGenerator.chart" -}}
{{- printf "%s-triples-generator" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tokenRepository.chart" -}}
{{- printf "%s-token-repository" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "knowledgeGraph.chart" -}}
{{- printf "%s-knowledge-graph" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
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

{{/*
Define subcharts full names
*/}}
{{- define "jena.fullname" -}}
{{- printf "%s-%s" .Release.Name "jena" | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
