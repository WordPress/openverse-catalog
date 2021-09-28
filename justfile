set dotenv-load := false

DEV_DOCKER_FILES := "--file=openverse_catalog/docker-compose.yml --file=openverse_catalog/docker-compose.override.yml"
SERVICE := "webserver"
TSV_FILE := "dags/provider_api_scripts/tests/resources/example_output/brooklynmuseum_1559050316.tsv"
TSV_FILE_NAME := "brooklynmuseum_"
TS := `date +%s`

install:
    pip install -r requirements.txt -r openverse_catalog/requirements_dev.txt
    pre-commit install


dotenv:
    @([ ! -f openverse_catalog/.env ] && cp openverse_catalog/env.template openverse_catalog/.env) || true


up flags="": dotenv
    docker-compose {{ DEV_DOCKER_FILES }} up -d {{ flags }}


down flags="":
    docker-compose {{ DEV_DOCKER_FILES }} down {{ flags }}


recreate:
    @just down -v
    @just up "--force-recreate --build"


logs: dotenv up
    docker-compose {{ DEV_DOCKER_FILES }} logs -f


test pytestargs="": dotenv up
    docker-compose {{ DEV_DOCKER_FILES }} exec {{ SERVICE }} /usr/local/airflow/.local/bin/pytest {{ pytestargs }}


shell: dotenv up
    docker-compose {{ DEV_DOCKER_FILES }} exec {{ SERVICE }} /bin/bash


airflow command="": dotenv up
    docker-compose {{ DEV_DOCKER_FILES }} exec {{ SERVICE }} airflow {{ command }}

loadtsv:
    docker exec -it openverse_catalog_webserver_1 /bin/bash && cp {{ TSV_FILE }} /tmp/{{ TSV_FILE_NAME }}{{TS}}.tsv && exit
