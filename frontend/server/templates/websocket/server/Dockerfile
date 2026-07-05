FROM node:22-alpine
WORKDIR /app
# Deps first so the npm-install layer survives server.js edits (same layering idea
# as the python service template's pip install).
COPY package.json .
RUN npm install --omit=dev
COPY server.js .
# 8080 = websocket traffic (reached through the tier's haproxy lb); 9100 = /metrics.
EXPOSE 8080 9100
CMD ["node", "server.js"]
