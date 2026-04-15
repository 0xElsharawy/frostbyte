# Default recipe to show available commands
default:
    @just --list

# Start all services in the background
up:
    @docker compose up -d

# Stop and remove containers, networks, and images
down:
    @docker compose down

# Stop and remove containers, networks, images, and volumes
down-all:
    @docker compose down -v --remove-orphans

# Show logs for a specific service (usage: just logs airflow)
logs service:
    @docker compose logs -f {{service}}

# Rebuild and restart services
rebuild:
    @docker compose up -d --build --remove-orphans

# Open a shell inside a running container (usage: just shell airflow)
shell service:
    @docker compose exec -it {{service}} bash

# Check status of containers
ps:
    @docker compose ps


terraform_dir := "./terraform"

# Initialize terraform
init:
    @cd {{terraform_dir}} && terraform init

# Apply terraform with auto-approve
apply:
    @cd {{terraform_dir}} && terraform apply -auto-approve

# Destroy terraform with auto-approve
destroy:
    @cd {{terraform_dir}} && terraform destroy -auto-approve
