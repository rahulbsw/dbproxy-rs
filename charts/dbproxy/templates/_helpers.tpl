{{- define "dbproxy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "dbproxy.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "dbproxy.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "dbproxy.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "dbproxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "dbproxy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dbproxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "dbproxy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dbproxy.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "dbproxy.configMapName" -}}
{{- default (include "dbproxy.fullname" .) .Values.config.existingConfigMap }}
{{- end }}

{{- define "dbproxy.secretName" -}}
{{- if .Values.secret.create }}
{{- include "dbproxy.fullname" . }}
{{- else }}
{{- required "secret.existingSecret is required when secret.create=false" .Values.secret.existingSecret }}
{{- end }}
{{- end }}

{{- define "dbproxy.portName" -}}
{{- $names := dict "mysql" "mysql" "postgresReadWrite" "pg-rw" "postgresReadOnly" "pg-ro" "redisReadWrite" "redis-rw" "redisReadOnly" "redis-ro" -}}
{{- get $names .name -}}
{{- end }}
