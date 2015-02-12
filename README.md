# Routing-Compiler

This application consists of a standalone server that controls the configuration of a RabbitMQ instance based on communication contracts.
The core function is 

  - receive new contracts
  - derive configuration for RabbitMQ: users, vhosts, exchanges, queues, bindings, shovels, policies
  - extract most recent existing configuration via the http management API of RabbitMQ
  - compare both configurations, derive elements to be deleted and elements that need to be added
  - apply new (delta) configuration via the management API
  
Other functions are a REST api to change contracts and a Graphviz DOT based visualization of configurations for debugging purposes.


## Usage


## License

Copyright © 2015 Steffen Dienst

Distributed under the Eclipse Public License, the same as Clojure.
