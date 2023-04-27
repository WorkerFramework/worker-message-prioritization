!not-ready-for-release!

#### Version Number
${version-number}

#### New Features
- None

#### Bug Fixes
- 691034: Added various fixes:  
  - Ensuring the application is shutdown when an unexpected error occurs instead of hanging.
  - Logging an ERROR when an exception occurs querying the RabbitMQ for a list of queues instead of shutting down the application.
  - Increasing the RabbitMQ connection and read timeouts from 10 seconds to 20 seconds.

#### Known Issues
- None
