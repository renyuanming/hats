<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

Horse Client
====================================



Implementation notes
--------------------
- [ ] Add a new load balancing policy for Horse, following the example of the `TokenAwarePolicy.newQueryPlan()`.
- [ ] Update the placement policy in the method `ControlConnection.refreshNodeListAndTokenMap()`.

- The files that related to the interaction between client and server
    - Cassandra server side:
        - Event.java
        - Server.java
        - Connection.java
        - ServerConnection.java



Building from source
--------------------

```
mvn -pl cassandra -am clean package -U -Dcheckstyle.skip
```


Links
-----
* To get here, use https://ycsb.site
* [Our project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)
