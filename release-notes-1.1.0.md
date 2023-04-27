!not-ready-for-release!

#### Version Number
${version-number}

#### New Features
- None

#### Bug Fixes
- 691034: Added various fixes:  
  - Ensure the application is shutdown when an exception occurs instead of hanging.
  - Log an ERROR when an exception occurs querying the RabbitMQ for a list of queues instead of shutting down the application.
  - Increasing the RabbitMQ connection and read timeouts from 10 seconds to 30 seconds.

#### Known Issues
- None
