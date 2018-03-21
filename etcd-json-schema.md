# JSON schema for etcd cluster configuration

Example JSON config doc:

```json
{
  "compose_deployment": "etcd-development-01",
  "endpoints": "https://dev3-9.compose.direct:15182,https://dev3-11.compose.direct:15182",
  "userid": "userid",
  "password": "password",
  "root_prefix": "aka-chroot-or-namespace",
  "certificate_file": "etcd-dev.pem"
}
```

- All attributes apart from `endpoints` are optional.
- The `root_prefix` attribute currently has **no effect** on clients created via `EtcdClientConfig.getClient()`. It's included in the configuration for use by application code (to query via `EtcdClientConfig.getRootPrefix()`). In future full chroot-like functionality at the client level might be supported.
- `certificate_file` is the name of a pem-format (public) cert to use for TLS server-auth, either an absolute path or a filename assumed to be in the same directory as the json config file itself.
- A `certificate` attribute may be included _instead of_ `certificate_file`, whose value is an embedded string UTF-8 pem format certificate. This allows a single json doc to hold all of the necessary connection info.
- The `compose_deployment` attribute is only required when using an IBM Compose etcd deployment with provided TLS certificates. It must be set to the name of the deployment, which is the CN of the TLS cert.

Example with embedded (trunctated) TLS cert:

```json
{
  "compose_deployment": "etcd-development-01",
  "endpoints": "https://dev3-9.compose.direct:15182,https://dev3-11.compose.direct:15182",
  "userid": "userid",
  "password": "password",
  "root_prefix": "aka-chroot-or-namespace",
  "certificate": "-----BEGIN CERTIFICATE-----\nMIIDaTCCA ... MP0u6J/xasx14IW4A==\n-----END CERTIFICATE-----\n"
}
```