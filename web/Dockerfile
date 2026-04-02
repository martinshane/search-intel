# ----------  build stage  ----------
FROM node:18-alpine AS builder

WORKDIR /app

# Install dependencies first (cache layer)
COPY web/package.json web/package-lock.json* ./
RUN npm ci --prefer-offline 2>/dev/null || npm install

# Copy source and build
COPY web/ .

# Build-time env vars — Railway injects these during build.
# NEXT_PUBLIC_API_URL is baked into the client bundle so the
# browser knows where the backend API lives.
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}

RUN npm run build


# ----------  runtime stage (slim)  ----------
FROM node:18-alpine

WORKDIR /app

# Copy only production artifacts
COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/next.config.js ./

# Railway injects PORT at runtime (default 3000 for Next.js)
ENV PORT=3000
ENV NODE_ENV=production
EXPOSE ${PORT}

CMD ["npx", "next", "start", "-p", "${PORT}"]
