# See https://docs.astral.sh/uv/guides/integration/aws-lambda/ for more details
FROM ghcr.io/astral-sh/uv:0.7.18 AS uv

# First, bundle the dependencies into the task root.
FROM public.ecr.aws/docker/library/python:3.13 as builder

# Enable bytecode compilation, to improve cold-start performance.
ENV UV_COMPILE_BYTECODE=1

# Disable installer metadata, to create a deterministic layer.
ENV UV_NO_INSTALLER_METADATA=1

# Enable copy mode to support bind mount caching.
ENV UV_LINK_MODE=copy

ENV LAMBDA_TASK_ROOT=/var/task

# Bundle the dependencies into the Lambda task root via `uv pip install --target`.
#
# Omit any local packages (`--no-emit-workspace`) and development dependencies (`--no-dev`).
# This ensures that the Docker layer cache is only invalidated when the `pyproject.toml` or `uv.lock`
# files change, but remains robust to changes in the application code.

RUN apt-get update && apt-get -y install rustc
COPY uv.lock pyproject.toml ./
RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt && \
    uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

FROM public.ecr.aws/docker/library/python:3.13

# This allows us to do streaming responses with Python
COPY --from=public.ecr.aws/awsguru/aws-lambda-adapter:0.9.1 /lambda-adapter /opt/extensions/lambda-adapter
ENV PORT=8080
ENV LAMBDA_TASK_ROOT=/var/task
# Copy the runtime dependencies from the builder stage.
COPY --from=builder ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}
# Needed for uvx
RUN pip install --no-cache-dir uv
# Copy the application code.
COPY ./app ${LAMBDA_TASK_ROOT}
WORKDIR ${LAMBDA_TASK_ROOT}

CMD python -m uvicorn --host 0.0.0.0 --port $PORT main:app
