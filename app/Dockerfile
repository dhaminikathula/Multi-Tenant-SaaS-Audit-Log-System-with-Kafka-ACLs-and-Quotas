FROM node:18-alpine

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install
RUN apk add --no-cache curl

COPY . .

EXPOSE 8080

CMD [ "node", "server.js" ]
