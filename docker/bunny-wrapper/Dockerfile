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

# Running as root temporarily because TES (via Funnel) mounts the outputs volume
# which requires root permissions to write to. 
# Once this is resolved, we can switch back to USER app.
# USER app

ENTRYPOINT [ "./entrypoint.sh" ]