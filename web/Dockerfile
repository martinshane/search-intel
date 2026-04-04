FROM node:18-alpine AS builder

WORKDIR /app

# Copy package files from build context root (which is web/ when root dir = /web)
COPY package.json package-lock.json* ./
RUN npm ci --prefer-offline 2>/dev/null || npm install

# Copy all source files
COPY . .

# Build-time env vars
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}

RUN npm run build

FROM node:18-alpine

WORKDIR /app

COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public 2>/dev/null || true
COPY --from=builder /app/next.config.js ./

ENV NODE_ENV=production

CMD ["npx", "next", "start", "-p", "3000"]
