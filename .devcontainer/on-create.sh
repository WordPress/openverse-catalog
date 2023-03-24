#!/bin/sh
python3 -m venv ../venv
sourve ../venv/bin/activate
just install
just build
just dotenv
