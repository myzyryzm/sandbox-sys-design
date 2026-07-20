/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_SYSTEM_ID: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
