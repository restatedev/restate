{{- define "restate.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "restate.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- $name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "restate.labels" -}}
{{- include "restate.selectorLabels" . }}
app.kubernetes.io/name: {{ include "restate.name" . }}
app.kubernetes.io/version: {{ .Values.version | default .Chart.Version | quote }}
{{- end }}

{{- define "restate.selectorLabels" -}}
app: {{ include "restate.fullname" . }}
{{- end }}

{{- define "restate.tag" -}}
{{- .Values.version | default .Chart.Version }}
{{- end }}
