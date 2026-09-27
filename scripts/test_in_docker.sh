#!/bin/sh
# Runs the test suite inside the Spark image (Java 17 + PySpark 3.5.5 already there), so no
# local Python/Java setup is needed. Test-only libraries go to /tmp, not into the image.
#
#   docker compose run --rm --no-deps -T --entrypoint sh spark-worker scripts/test_in_docker.sh
#   docker compose run --rm --no-deps -T --entrypoint sh spark-worker scripts/test_in_docker.sh tests/slice_c -v
set -e
pip install -q --disable-pip-version-check --target /tmp/testlibs \
    pytest==8.3.4 httpx==0.28.1 fastapi==0.115.6 confluent-kafka==2.6.1 2>&1 | grep -v "^WARNING" || true
PY4J=$(ls /opt/spark/python/lib/py4j-*.zip)
export PYTHONPATH="/tmp/testlibs:/opt/spark/python:${PY4J}:/opt/project"
export PYTHONDONTWRITEBYTECODE=1
cd /opt/project
exec python3 -m pytest -p no:cacheprovider "$@"
