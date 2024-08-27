!not-ready-for-release!

#### Version Number
${version-number}

#### New Features
- US929026: Updated to run on Java 21.
- US946052: Target queues will now be created if they do not already exist.
- US952036: Image is now built on Oracle Linux.
- US957002: Added new `CAF_WMP_KUBERNETES_ENABLED` environment variable.  
  Used to specify whether Kubernetes is enabled. If `false`, then fallback settings (for information about a worker's
  target queue such as its maximum length) will be used in place of the labels that would have been retrieved from Kubernetes. Defaults
  to `true`.

#### Known Issues
