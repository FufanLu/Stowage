{{- define "stowage.name" -}}{{ .Release.Name }}-stowage{{- end -}}
{{- define "stowage.labels" -}}
app.kubernetes.io/name: stowage
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{/* 1=<pod-0 dns>:<port>,2=… for every node. */}}
{{- define "stowage.peers" -}}
{{- $name := include "stowage.name" .ctx -}}
{{- $ns := .ctx.Release.Namespace -}}
{{- $port := .port -}}
{{- $list := list -}}
{{- range $i := until (int .ctx.Values.replicas) -}}
{{- $list = append $list (printf "%d=%s-%d.%s-hl.%s.svc.cluster.local:%d" (add1 $i) $name $i $name $ns (int $port)) -}}
{{- end -}}
{{- join "," $list -}}
{{- end -}}
