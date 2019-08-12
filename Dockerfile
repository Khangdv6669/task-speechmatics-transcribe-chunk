FROM node:8.12

ADD . /app
COPY manifest.json /var/
WORKDIR /app

RUN npm install
RUN npm rebuild sleep --force
RUN chmod +x "/app/run.sh"

ENV VERITONE_WEBHOOK_READY="http://0.0.0.0:8080/readyz"
ENV VERITONE_WEBHOOK_PROCESS="http://0.0.0.0:8080/process"

ENTRYPOINT ["/app/run.sh"]
