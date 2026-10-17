FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/sniper ./cmd/server && mkdir -p /out/data

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /out/sniper /app/sniper
COPY --from=build --chown=nonroot:nonroot /out/data /app/data
# Mount a persistent volume at /app/data for SQLite in production.
VOLUME ["/app/data"]
ENV HTTP_ADDR=:8080
ENV DATABASE_PATH=/app/data/sniper.db
EXPOSE 8080
ENTRYPOINT ["/app/sniper"]
