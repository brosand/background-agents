# =============================================================================
# Kubernetes Configuration
# =============================================================================

variable "kubeconfig_path" {
  description = "Path to kubeconfig file"
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Kubernetes context to use"
  type        = string
  default     = ""
}

variable "namespace" {
  description = "Kubernetes namespace for deployment"
  type        = string
  default     = "open-inspect"
}

# =============================================================================
# Domain Configuration
# =============================================================================

variable "control_plane_domain" {
  description = "Domain for the control plane API"
  type        = string
}

variable "web_app_domain" {
  description = "Domain for the web application"
  type        = string
}

# =============================================================================
# Rivet Configuration
# =============================================================================

variable "rivet_api_url" {
  description = "Rivet API URL for sandbox orchestration"
  type        = string
}

variable "rivet_token" {
  description = "Rivet API token"
  type        = string
  sensitive   = true
}

variable "rivet_project" {
  description = "Rivet project ID"
  type        = string
}

variable "rivet_environment" {
  description = "Rivet environment (e.g., production)"
  type        = string
  default     = "production"
}

# =============================================================================
# GitHub Configuration
# =============================================================================

variable "github_client_id" {
  description = "GitHub OAuth App client ID"
  type        = string
}

variable "github_client_secret" {
  description = "GitHub OAuth App client secret"
  type        = string
  sensitive   = true
}

variable "github_app_id" {
  description = "GitHub App ID"
  type        = string
  default     = ""
}

variable "github_app_private_key" {
  description = "GitHub App private key (PEM format)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "github_app_installation_id" {
  description = "GitHub App installation ID"
  type        = string
  default     = ""
}

# =============================================================================
# Secrets
# =============================================================================

variable "token_encryption_key" {
  description = "AES-256 key for encrypting OAuth tokens (hex-encoded)"
  type        = string
  sensitive   = true
}

variable "repo_secrets_encryption_key" {
  description = "AES-256 key for encrypting repository secrets (hex-encoded)"
  type        = string
  sensitive   = true
}

variable "internal_callback_secret" {
  description = "HMAC secret for service-to-service authentication"
  type        = string
  sensitive   = true
}

variable "nextauth_secret" {
  description = "NextAuth.js session secret"
  type        = string
  sensitive   = true
}

# =============================================================================
# Database
# =============================================================================

variable "db_password" {
  description = "PostgreSQL password for the open_inspect user"
  type        = string
  sensitive   = true
}

# =============================================================================
# Container Images
# =============================================================================

variable "control_plane_image" {
  description = "Control plane container image"
  type        = string
  default     = "open-inspect/control-plane:latest"
}

variable "web_image" {
  description = "Web frontend container image"
  type        = string
  default     = "open-inspect/web:latest"
}
