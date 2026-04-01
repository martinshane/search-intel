# ---------- deps stage (install node_modules) ----------
FROM node:18-alpine AS deps

# Add libc6-compat for alpine compatibility with native node modules
RUN apk add --no-cache libc6-compat

WORKDIR /app

# Copy package files and install dependencies
COPY web/package.json web/package-lock.json* web/yarn.lock* web/pnpm-lock.yaml* ./
RUN \
  if [ -f yarn.lock ]; then yarn install --frozen-lockfile; \
  elif [ -f pnpm-lock.yaml ]; then corepack enable pnpm && pnpm install --frozen-lockfile; \
  elif [ -f package-lock.json ]; then npm ci; \
  else npm install; \
  fi


# ---------- builder stage (build Next.js app) ----------
FROM node:18-alpine AS builder

WORKDIR /app

# Copy deps from previous stage
COPY --from=deps /app/node_modules ./node_modules

# Copy web source (from repo root context, path = web/*)
COPY web/ ./

# Set build-time env vars
# NEXT_PUBLIC_API_URL must be set at build time for static pages
# Railway injects this via service env vars
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}

# Enable Next.js standalone output for minimal production image
ENV NEXT_TELEMETRY_DISABLED=1

# Build the app
RUN npm run build


# ---------- runner stage (production) ----------
FROM node:18-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Create non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy the standalone build output (if standalone is enabled)
# Otherwise copy the full .next directory
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/next.config.js ./next.config.js

USER nextjs

# Railway injects $PORT at runtime
ENV PORT=3000
EXPOSE ${PORT}

CMD ["npm", "start"]
