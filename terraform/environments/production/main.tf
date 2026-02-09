# =============================================================================
# Open-Inspect - Production Environment (Kubernetes + Rivet)
# =============================================================================
# Deploys:
# - Kubernetes namespace and secrets
# - Helm release for control plane, web frontend, PostgreSQL, and Redis
# =============================================================================

# Namespace
resource "kubernetes_namespace" "open_inspect" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name"       = "open-inspect"
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
}

# Database URL constructed from Helm-managed PostgreSQL
locals {
  db_host = "${var.namespace}-open-inspect-postgresql"
  db_url  = "postgresql://open_inspect:${var.db_password}@${local.db_host}:5432/open_inspect"

  control_plane_image_parts = split(":", var.control_plane_image)
  control_plane_image_repo  = local.control_plane_image_parts[0]
  control_plane_image_tag   = length(local.control_plane_image_parts) > 1 ? local.control_plane_image_parts[1] : "latest"

  web_image_parts = split(":", var.web_image)
  web_image_repo  = local.web_image_parts[0]
  web_image_tag   = length(local.web_image_parts) > 1 ? local.web_image_parts[1] : "latest"
}

# Control Plane secrets
resource "kubernetes_secret" "control_plane" {
  metadata {
    name      = "open-inspect-control-plane"
    namespace = kubernetes_namespace.open_inspect.metadata[0].name
  }

  data = {
    "database-url"                = local.db_url
    "rivet-api-url"               = var.rivet_api_url
    "rivet-token"                 = var.rivet_token
    "token-encryption-key"        = var.token_encryption_key
    "repo-secrets-encryption-key" = var.repo_secrets_encryption_key
    "internal-callback-secret"    = var.internal_callback_secret
    "github-client-id"            = var.github_client_id
    "github-client-secret"        = var.github_client_secret
    "github-app-id"               = var.github_app_id
    "github-app-private-key"      = var.github_app_private_key
    "github-app-installation-id"  = var.github_app_installation_id
  }

  type = "Opaque"
}

# Web secrets
resource "kubernetes_secret" "web" {
  metadata {
    name      = "open-inspect-web"
    namespace = kubernetes_namespace.open_inspect.metadata[0].name
  }

  data = {
    "nextauth-secret"          = var.nextauth_secret
    "github-client-id"         = var.github_client_id
    "github-client-secret"     = var.github_client_secret
    "internal-callback-secret" = var.internal_callback_secret
  }

  type = "Opaque"
}

# PostgreSQL password secret (for Bitnami chart)
resource "kubernetes_secret" "db" {
  metadata {
    name      = "open-inspect-db"
    namespace = kubernetes_namespace.open_inspect.metadata[0].name
  }

  data = {
    "postgres-password" = var.db_password
    "password"          = var.db_password
  }

  type = "Opaque"
}

# Helm release
resource "helm_release" "open_inspect" {
  name       = "open-inspect"
  namespace  = kubernetes_namespace.open_inspect.metadata[0].name
  chart      = "${path.module}/../../../helm"
  wait       = true
  timeout    = 600

  values = [
    yamlencode({
      controlPlane = {
        image = {
          repository = local.control_plane_image_repo
          tag        = local.control_plane_image_tag
        }
      }

      web = {
        image = {
          repository = local.web_image_repo
          tag        = local.web_image_tag
        }
      }

      rivet = {
        apiUrl         = var.rivet_api_url
        project        = var.rivet_project
        environment    = var.rivet_environment
        sandboxBuildTag = "latest"
      }

      ingress = {
        controlPlane = {
          host = var.control_plane_domain
        }
        web = {
          host = var.web_app_domain
        }
      }
    })
  ]

  depends_on = [
    kubernetes_secret.control_plane,
    kubernetes_secret.web,
    kubernetes_secret.db,
  ]
}
