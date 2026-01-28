ARG BUNNY_VERSION=1.6
FROM ghcr.io/health-informatics-uon/hutch/bunny:${BUNNY_VERSION}

# Specify non-CLI "required" config so users don't have to
ENV TASK_API_BASE_URL https://example.com
ENV TASK_API_USERNAME username
ENV TASK_API_PASSWORD password
ENV COLLECTION_ID collection_id

# Add custom entrypoint
USER root
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# run the entrypoint as the base image's non-root user
USER app
ENTRYPOINT [ "./entrypoint.sh" ]