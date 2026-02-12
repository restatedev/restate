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
app.kubernetes.io/version: {{ include "restate.tag" . | quote }}
{{- end }}

{{- define "restate.selectorLabels" -}}
app: {{ include "restate.fullname" . }}
{{- end }}

{{/*
Image tag to use. Prefers image.tag, falls back to version (deprecated), then Chart.Version.
*/}}
{{- define "restate.tag" -}}
{{- .Values.image.tag | default .Values.version | default .Chart.Version }}
{{- end }}

{{/*
Full image reference. Uses digest if specified, otherwise uses tag.
Note: If both image.digest and image.tag are specified, image.digest takes precedence.
*/}}
{{- define "restate.image" -}}
{{- if and .Values.image.digest (not (hasPrefix "sha256:" .Values.image.digest)) -}}
{{- fail "image.digest must start with 'sha256:'" -}}
{{- end -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ include "restate.tag" . }}
{{- end -}}
{{- end }}
