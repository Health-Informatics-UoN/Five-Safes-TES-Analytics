FROM ghcr.io/health-informatics-uon/hutch/bunny:1.6

USER root
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER app

ENTRYPOINT [ "./entrypoint.sh" ]