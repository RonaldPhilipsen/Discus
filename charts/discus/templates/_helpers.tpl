{{- define "discus.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Truncated to 57 characters, so the "-peers" suffix still fits a Service name. */}}
{{- define "discus.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 57 | trimSuffix "-" }}
{{- else if contains (include "discus.name" .) .Release.Name }}
{{- .Release.Name | trunc 57 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "discus.name" .) | trunc 57 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "discus.selectorLabels" -}}
app.kubernetes.io/name: {{ include "discus.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "discus.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "discus.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "discus.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "discus.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
