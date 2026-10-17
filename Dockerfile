FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -o /out/sniper ./cmd/server

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /out/sniper /app/sniper
ENV HTTP_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/app/sniper"]
