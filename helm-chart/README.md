## Renku Graph Helm Chart

Provides a basic chart to deploy Renku Graph services.

### Usage

To be executed in the `helm-chart` directory:

```bash
helm upgrade --install -f minikube-values.yaml renku-graph renku-graph
```

### Notice

Due to security reasons (more [here](https://docs.gitlab.com/ee/security/webhooks.html)) webhooks urls on GitLab cannot point to local addresses. In order to make `renku-graph` services be able to create webhooks on GitLab automatically, `Allow requests to the local network from hooks and services` checkbox has to be enabled. The checkbox can be found at `http://192.168.99.100/gitlab/admin/application_settings/network#js-outbound-settings`.