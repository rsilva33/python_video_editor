# Utilizando uma imagem oficial do Golang para desenvolvimento
FROM golang:1.23-alpine

RUN apk add --no-cache git bash ffmpeg

WORKDIR /app

COPY . .

CMD [ "bash" ]