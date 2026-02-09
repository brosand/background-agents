# =============================================================================
# Infrastructure Outputs
# =============================================================================

output "namespace" {
  description = "Kubernetes namespace"
  value       = kubernetes_namespace.open_inspect.metadata[0].name
}

output "control_plane_url" {
  description = "Control plane API URL"
  value       = "https://${var.control_plane_domain}"
}

output "web_app_url" {
  description = "Web application URL"
  value       = "https://${var.web_app_domain}"
}

output "helm_release_status" {
  description = "Helm release status"
  value       = helm_release.open_inspect.status
}
