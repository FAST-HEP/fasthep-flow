# Credentials

Credentials allow distributed workers to access protected resources.

Examples include:

* remote datasets,
* storage systems,
* experiment infrastructure,
* private XRootD endpoints.

FAST-HEP treats credentials as part of the execution infrastructure rather than the analysis itself.

## Why Credentials Are Separate

A workflow should describe:

```text
What data to process.
```

It should not need to describe:

```text
How a worker authenticates.
```

For example:

```yaml
datasets:
  - name: data
    files:
      - root://my-storage.example.org/path/file.root
```

Whether access requires authentication depends on the storage system, not on the analysis logic.

## X509 Proxies

FAST-HEP currently supports transferring X509 proxies to distributed workers.

A typical workflow is:

```text
Create proxy
    ↓
Submit workflow
    ↓
Transfer proxy to workers
    ↓
Configure X509_USER_PROXY
    ↓
Access remote data
```

On the submit host:

```bash
voms-proxy-init ...
```

or:

```bash
export X509_USER_PROXY=/tmp/x509up_u12345
```

The execution system can transfer this proxy to worker nodes and configure the worker environment automatically.

## Worker Configuration

When X509 support is enabled:

1. the proxy file is transferred to the worker,
2. the worker configures:

```bash
export X509_USER_PROXY=<worker proxy path>
```

3. analysis code can access protected resources normally.

The workflow itself does not need to know where the proxy is stored.

## Public Data

Credentials are not always required.

For example, CMS Open Data files can be accessed directly:

```text
root://eospublic.cern.ch//eos/opendata/cms/...
```

A workflow developed using public datasets can often be switched to private datasets simply by changing the dataset definition.

The execution infrastructure handles the authentication details.

## Troubleshooting

A few useful checks when debugging credential-related issues:

Check that a proxy exists:

```bash
echo "$X509_USER_PROXY"
```

Inspect the proxy:

```bash
voms-proxy-info --all
```

Check remaining lifetime:

```bash
voms-proxy-info --timeleft
```

On a worker node:

```bash
echo "$X509_USER_PROXY"
```

Verify that the environment variable points to the transferred proxy file.
