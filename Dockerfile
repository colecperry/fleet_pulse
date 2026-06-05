# This Dockerfile packages the Fleet Pulse API into a container image.
# It isn't wired to a CI/CD pipeline or a live deployment target — the project runs locally. The Dockerfile exists so the API is deployment-ready if that ever changes, and to demonstrate production container practices as a portfolio piece.

# =============================================================================
# Stage 1 — builder
# =============================================================================
# We compile TypeScript here and nothing from this stage ships to production.
# All devDependencies (ts-node, nodemon, jest, type stubs, etc.) and the raw TypeScript source files are LEFT BEHIND in this stage. The final image only receives the compiled JavaScript output, which is why multi-stage builds dramatically reduce image size and attack surface.
# =============================================================================
# Start from an official Node.js base image
FROM node:22-alpine AS builder

# Set the working directory in the container to /app
WORKDIR /app 

# copy only package.json and package-lock.json into the container first to leverage Docker layer caching which cache's each instruction as a layer and re runs only if files change
COPY package*.json ./

# Install ALL dependencies (including devDependencies Jest, TypeScript, nodemon, etc) needed to compile, but none of it will make it to the final production image
RUN npm ci

# Copy TypeScript source and compiler config.
COPY src/ ./src/
COPY tsconfig.json ./

# Compile TypeScript to JavaScript. Output lands in ./dist per tsconfig outDir.
# The TypeScript compiler and every devDependency installed above stays here
# in the builder stage — none of it is copied forward.
RUN npx tsc

# =============================================================================
# Stage 2 — production
# =============================================================================
# Fresh base image. Nothing from Stage 1 is present except what we explicitly copy with COPY --from=builder. This means:
#   - No TypeScript source (.ts files)
#   - No devDependencies (ts-node, jest, nodemon, type stubs, etc.)
#   - No TypeScript compiler toolchain
# The result is a lean image that contains only what is needed to run the app.
# =============================================================================
FROM node:22-alpine AS production

WORKDIR /app

# Copy dependency manifests so we can install production deps only.
COPY package*.json ./

# Install only production dependencies (no devDependencies).
# --omit=dev ensures build tools, test frameworks, and type stubs are excluded.
RUN npm ci --omit=dev

# Copy compiled JavaScript from the builder stage.
# This is the only artifact we bring forward — compiled JS output, no TS source.
COPY --from=builder /app/dist ./dist

# =============================================================================
# Non-root user
# =============================================================================
# By default Docker containers run as root (UID 0). If an attacker exploits a vulnerability in the application they would have root access inside the container, making it easier to escape to the host or escalate privileges.
# Switching to a non-root user limits the blast radius: a compromised process cannot write to system directories, install packages, or bind privileged ports — it is constrained to only what the "node" user can access.
#
# The node:22-alpine base image ships with a built-in "node" user (UID 1000).
# We use it here rather than creating a new one so we do not need adduser.
# =============================================================================
USER node

# Declare the port the application listens on. This is metadata for tooling
# (docker run -P, orchestrators, documentation) — it does not publish the port.
EXPOSE 3000

# Start the compiled application directly with Node
CMD ["node", "dist/index.js"]
