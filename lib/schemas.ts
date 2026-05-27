import { type } from 'arkenv'

export const envStorageDriverSchema = type.or(
  {
    'STORAGE_DRIVER': type.unit('s3'),
    'STORAGE_S3_BUCKET': 'string',
    'AWS_REGION': "string = 'us-east-1'",
    'AWS_ENDPOINT_URL?': 'string.url',
    'AWS_ACCESS_KEY_ID?': 'string',
    'AWS_SECRET_ACCESS_KEY?': 'string',
  },
  {
    STORAGE_DRIVER: type.unit('filesystem'),
    STORAGE_FILESYSTEM_PATH: 'string',
  },
  {
    'STORAGE_DRIVER': type.unit('gcs'),
    'STORAGE_GCS_BUCKET': 'string',
    'STORAGE_GCS_SERVICE_ACCOUNT_KEY?': 'string',
    'STORAGE_GCS_ENDPOINT?': 'string.url',
  },
)
export const envDbDriverSchema = type.or(
  type.or(
    {
      'DB_DRIVER': type.unit('postgres'),
      'DB_POSTGRES_DATABASE': 'string',
      'DB_POSTGRES_HOST': 'string',
      'DB_POSTGRES_PORT': 'number.port',
      'DB_POSTGRES_USER': 'string',
      'DB_POSTGRES_PASSWORD': 'string',
      'DB_POSTGRES_URL?': 'undefined',
    },
    {
      'DB_DRIVER': type.unit('postgres'),
      'DB_POSTGRES_URL': 'string',
      'DB_POSTGRES_DATABASE?': 'undefined',
      'DB_POSTGRES_HOST?': 'undefined',
      'DB_POSTGRES_PORT?': 'undefined',
      'DB_POSTGRES_USER?': 'undefined',
      'DB_POSTGRES_PASSWORD?': 'undefined',
    },
  ),
  {
    DB_DRIVER: type.unit('mysql'),
    DB_MYSQL_DATABASE: 'string',
    DB_MYSQL_HOST: 'string',
    DB_MYSQL_PORT: 'number.port',
    DB_MYSQL_USER: 'string',
    DB_MYSQL_PASSWORD: 'string',
  },
  {
    DB_DRIVER: type.unit('sqlite'),
    DB_SQLITE_PATH: 'string',
  },
)

export const envBaseSchema = type({
  'API_BASE_URL': 'string.url',
  'DEFAULT_ACTIONS_RESULTS_URL':
    "string.url = 'https://results-receiver.actions.githubusercontent.com'",
  'CACHE_CLEANUP_OLDER_THAN_DAYS': 'number = 90',
  'CACHE_PENDING_WAIT_TIMEOUT_MS': 'number = 300000',
  'DISABLE_CLEANUP_JOBS?': 'boolean',
  'DEBUG?': 'unknown',
  'ENABLE_DIRECT_DOWNLOADS': 'boolean = false',
  'BENCHMARK': 'boolean = false',
  'SKIP_TOKEN_VALIDATION': 'boolean = false',
  'MANAGEMENT_API_KEY?': 'string',
  'STORAGE_FILESYSTEM_CACHE_PATH?': 'string',
  'STORAGE_FILESYSTEM_CACHE_MAX_SIZE_BYTES': 'number = 10737418240',
  'STORAGE_FILESYSTEM_CACHE_MAX_OBJECT_SIZE_BYTES': 'number = 1073741824',
  'DOCKERHUB_MIRROR_ENABLED': 'boolean = true',
  'DOCKERHUB_REGISTRY_URL': "string.url = 'https://registry-1.docker.io'",
  'DOCKERHUB_AUTH_URL': "string.url = 'https://auth.docker.io/token'",
  'DOCKERHUB_USERNAME?': 'string',
  'DOCKERHUB_PASSWORD?': 'string',
  'DOCKERHUB_MANIFEST_TTL_SECONDS': 'number = 300',
  'DOCKERHUB_PENDING_WAIT_TIMEOUT_MS': 'number = 300000',
})

export const envSchema = envBaseSchema.and(envStorageDriverSchema).and(envDbDriverSchema)
export type Env = typeof envSchema.infer
