set dotenv-load := false

# Show all available recipes
default:
  @just --list --unsorted

DEV_DOCKER_FILES := "--file=docker-compose.yml --file=docker-compose.override.yml"
SERVICE := "webserver"


# Install dependencies into the current environment
install:
    pip install -r requirements.txt -r requirements_dev.txt
    pre-commit install

# Create the .env file from the template
dotenv:
    @([ ! -f .env ] && cp env.template .env) || true

# Build all (or specified) container(s)
build service="":
    docker-compose {{ DEV_DOCKER_FILES }} build {{ service }}

# Bring all Docker services up
up flags="": dotenv
    docker-compose {{ DEV_DOCKER_FILES }} up -d {{ flags }}

# Take all Docker services down
down flags="":
    docker-compose {{ DEV_DOCKER_FILES }} down {{ flags }}

# Recreate all volumes and containers from scratch
recreate: dotenv
    @just down -v
    @just up "--force-recreate --build"

# Show logs of all, or named, Docker services
logs: up
    docker-compose {{ DEV_DOCKER_FILES }} logs -f

# Run pre-commit on all files
lint:
    pre-commit run --all-files

# Run pytest using the webserver image
test pytestargs="": up
    docker-compose {{ DEV_DOCKER_FILES }} run --rm {{ SERVICE }} /usr/local/airflow/.local/bin/pytest {{ pytestargs }}

# Open a shell into the webserver container
shell: up
    docker-compose {{ DEV_DOCKER_FILES }} exec {{ SERVICE }} /bin/bash

# Run a given airflow command using the webserver image
airflow command="": up
    docker-compose {{ DEV_DOCKER_FILES }} exec {{ SERVICE }} airflow {{ command }}
