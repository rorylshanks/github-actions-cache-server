# 🚀 GitHub Actions Cache Server

This is a drop-in replacement for the official GitHub hosted cache server. It is compatible with the official `actions/cache` action, so there is no need to change your workflow files and it even works with packages that internally use `actions/cache`.

## Features

- 🔥 **Compatible with official `actions/cache` action**
- 📦 Supports multiple storage solutions and is easily extendable.
- 🐳 Can act as a Docker Hub registry mirror at `/v2/*`.
- 💾 Optional local filesystem writeback/LRU cache in front of object storage.
- 🔒 Secure and self-hosted, giving you full control over your cache data.
- 😎 Easy setup

```yaml
services:
  cache-server:
    image: ghcr.io/falcondev-oss/github-actions-cache-server
    ports:
      - '3000:3000'
    environment:
      API_BASE_URL: http://localhost:3000
      STORAGE_DRIVER: filesystem
      STORAGE_FILESYSTEM_PATH: /data/cache
      DB_DRIVER: sqlite
      DB_SQLITE_PATH: /data/cache-server.db
    volumes:
      - cache-data:/data

volumes:
  cache-data:
```

## Documentation

👉 <https://gha-cache-server.falcondev.io/getting-started> 👈

## Docker Hub mirror and local storage cache

The Docker Registry v2 mirror is available at `/v2/*` and is enabled by default. Configure Docker with this server as a registry mirror for `docker.io`; blobs and manifests are stored through the configured storage backend, and concurrent pulls of the same uncached object wait on the same fill.

For an S3/Postgres deployment with a local filesystem cache, keep `STORAGE_DRIVER=s3` and `DB_DRIVER=postgres`, then add:

```yaml
STORAGE_FILESYSTEM_CACHE_PATH: /data/local-object-cache
STORAGE_FILESYSTEM_CACHE_MAX_SIZE_BYTES: 10737418240
STORAGE_FILESYSTEM_CACHE_MAX_OBJECT_SIZE_BYTES: 1073741824
```

Uploads commit to the filesystem cache first and write back to the backing object store asynchronously. Cache entries are visible in the database immediately as pending, so concurrent restores wait until the local fill is available.
