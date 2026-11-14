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

{{- /*
One value of settings.extra as TOML: an integer, a boolean, a string, or an array of those. The
schema refuses other values too; this check also holds with --skip-schema-validation.
Arguments: (list "settings.extra.section.key" value).
*/ -}}
{{- define "discus.tomlValue" -}}
{{- $path := index . 0 -}}
{{- $value := index . 1 -}}
{{- if kindIs "slice" $value -}}
{{- $items := list -}}
{{- range $value -}}
{{- $items = append $items (include "discus.tomlScalar" (list $path .)) -}}
{{- end -}}
[{{ join ", " $items }}]
{{- else -}}
{{- include "discus.tomlScalar" (list $path $value) -}}
{{- end -}}
{{- end }}

{{- define "discus.tomlScalar" -}}
{{- $path := index . 0 -}}
{{- $value := index . 1 -}}
{{- if or (kindIs "bool" $value) (kindIs "string" $value) -}}
{{- toJson $value -}}
{{- else if or (kindIs "int" $value) (kindIs "int64" $value) -}}
{{- $value -}}
{{- else if and (kindIs "float64" $value) (eq $value (float64 (int64 $value))) -}}
{{- int64 $value -}}
{{- else -}}
{{- fail (printf "%s must be an integer, a boolean, a string, or an array of those" $path) -}}
{{- end -}}
{{- end }}
