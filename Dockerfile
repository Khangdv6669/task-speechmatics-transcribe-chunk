FROM node:10.2-alpine

ADD . /app
COPY manifest.json /var/
WORKDIR /app
RUN mv .netrc $HOME \
&& apk add --update git
RUN apk add --no-cache --virtual .gyp \
        python \
        make \
        g++
RUN npm install
RUN npm rebuild sleep --force
RUN rm $HOME/.netrc \
&& chmod +x "/app/run.sh"

ENTRYPOINT ["/app/run.sh"]
