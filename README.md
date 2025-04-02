# Redis Data Migrator

A Java utility for migrating data from Redis to Aerospike.

## Features

- Import data from Redis RDB backup files into Aerospike
- Configurable data mapping between Redis and Aerospike
- Support for Redis data types like strings, lists, sets, hashes
- TLS/SSL support for secure connections
- Command line interface for easy usage

## Requirements

- Java 11 or later
- Maven for building
- Redis RDB backup file
- Aerospike database (version 7 or later recommended)

## Installation

Clone the repository and build using Maven:

```
git clone https://github.com/aerospike-examples/redis-data-migrator.git
cd redis-data-migrator
mvn clean package
```

## Usage
```
-a,--authMode <arg>              Set the auth mode of Aerospike cluster. Default: INTERNAL
-cn,--clusterName <arg>          Set the cluster name of the Aerospike cluster
-D,--debug                       Turn on debug mode. This will output a lot of information and automatically turn on verbose mode and turn
                                 silent mode off
-ef,--errorFile <arg>            Name of file to write errors to, in addtion to stdout
-h,--host <arg>                  List of seed hosts for first cluster in format: hostname1[:tlsname][:port1],...
                                 The tlsname is only used when connecting with a secure TLS enabled server. If the port is not specified,
                                 the default port is used. IPv6 addresses must be enclosed in square brackets.
                                 Default: localhost
                                 Examples:
                                 host1
                                 host1:3000,host2:3000
                                 192.168.1.10:cert1:3000,[2001::1111]:cert2:3000
-i,--inputFile <arg>             Path to a RDB file to import.
-im,--ignoreMissing              If a record in Redis has a key which does not match any of the mapping specs, silently ignore this record
                                 instead of flagging an error.
-m,--mappingFile <arg>           YAML file with mappings in it. Every string key in Redis must be mapped to a (namespace, set, id) tuple in
                                 Aerospike. This file specifies these mappings using regular expressions. This file is required
-P,--password <arg>              Password for cluster
-q,--quiet                       Do not output spurious information like progress.
-qd,--queueDepth <arg>           Specify the maximum queue depth to process from file. (Default: 5000)
-rea,--recordExistsAction <arg>  Action to take if the record already exists in Aerospike. Values include:
                                 * UPDATE (default) - records are upserted, merging in with existing records.
                                 * REPLACE - record contents become the values of the last update from Redis* CREATE_ONLY - only insert
                                 records, never overwrite or merge with existing records
-sa,--useServicesAlternate       Use services alternative when connecting to the Aerospike cluster
-sk,--sendKey <arg>              Whether to send the key to the server on each request. Defaults to true
-t,--threads <arg>               Number of threads to use. Use 0 to use 1 thread per core. (Default: 0)
-ts,--tls <arg>                  Set the TLS Policy options for the Aerospike cluster. The value passed should be a JSON string. Valid keys
                                 in this string inlcude 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'. For 'context', the
                                 value should be a JSON string which can contain keys 'certChain' (path to the certificate chain PEM),
                                 'privateKey' (path to the certificate private key PEM), 'caCertChain' (path to the CA certificate PEM),
                                 'keyPassword' (password used for the certificate chain PEM), 'tlsHost' (the tlsName of the Aerospike host).
                                 For example: --tls
                                 '{"context":{"certChain":"cert.pem","privateKey":"key.pem","caCertChain":"cacert.pem","tlsHost":"tls1"}}'
-u,--usage                       Display the usage and exit.
-U,--user <arg>                  User name for cluster
-V,--verbose                     Turn on verbose logging, especially for cluster details and TLS connections
```

For example, to connect to a cluster on the local machine listening on port 3000 (the default), and importing data from a Redis file called `dump.rdb`, the following command line can be used: 
```
java -jar target/redis-data-migrator-0.9-full.jar -m "mapping.yaml" -i dump.rdb -h localhost:3000
```

## Mapping File
The mapping file defines how the data is mapped from Redis to Aerospike. This is a mandoatory file as the data layouts are different between the two databases.
|Database|Layout|
|-|-|
|Redis|Redis database contains no inherent structure like databases or tables. Instead, keys are standalone and each key has a single value. Structure is likely imposed by the application so keys may include table names, ids and so on. For example `customer:555:account:1`|
|Aerospike|Aerospike stores records similar to a relational database with some structure. Each key contains three distinct parts: a namespace, a set and an id. A `namespace` is like a tablespace or database is relational concepts, with a limited number is the system. A `set` is analogous to a table is relational terms, abd tge id is the id of the row in the table (primary key in relational concepts). These can be strings, integers or byte arrays.|

Hence, the key in Redis must be translated into the three components in Aerospike. This is normally fairly obvious to the data architect, and some parts of the keys might be hard coded. For example, `customer:555` might be translated to `namespace = "test", set = "customer", id = 555`. The mapping file consists of a list of mappings, and uses regular expressions to extract parts of the id.

For example, consider an object in Redis stored in a `hash`. This is a sequence of key/value pairs, all of which are strings. Several key/value pairs may be set in a single command, such as 
```
hset account:1 name "Savings 1" type Savings balance 300
```
This creates a hash with a key of `account:1` and 3 fields: `name`, `type` and `balance` with the respective values. In order to translate this to the namespace "test" in the Account set, a translation rule like the following could be used:
```
- key: account:(\d+)
  namespace: test
  set: Account
  id: $1
```
The `key` part specifies a regular expression to match. If it matches, then the rest of the section applies. The converter checks each of the expressions listed in the mapping file agains the received key in the order they appear in the mapping file. So if there are situations where multiple `key` sections may match, make sure to put the most important ones earlier in the file. If no expressions match, then an error will be logged for that key, unless the `--ignoreMissing` flag is specfied. 

For this key, the `namesapce`, `set` and `id` must be specified. In this case the namespace and set are hard coded, but the id comes from the passed value. `$1` specifies the first group captured in the key, in this case the `\d+` at the end, representing the digits in the string. 

The full syntax of the mapping file is:

|Field|Required|Use|
|-|-|-|
|`key`|Yes|The key(s) to match in the Redis file. Regular expressions are allowed, and captured groups in the regular expressions may be used in other fields under this section. |
|`namespace`|Yes|The namespace this key maps to|
|`set`|Yes|The set to store this record in Aerospike|
|`id`|Yes|The id of the Aerospike record. The id, set and namespace form a tuple used to uniquely identify a record in Aerospike|
|`path`|No|A path denotes where in that record to store this peices of data. See more on paths below. These paths do not support wildcards.|
|`sendKey`|No|If set, determine whether to send the key for this record. This is finer grained (and overrides) the value which can be set with the `--sendKey` command-line argument. If neither is set, the key will be set by default. |
|`type`|No|The type of the record key. All keys in Redis are strings, but Aerospikes supports `STRING`, `BLOB` and `INTEGER` keys. If `INTEGER` is passed, the key will be converted to a number before the record is stored. If `BLOB` is specified, the key should represent hexadecimal digits and will be converted into a byte array before being stored. `STRING` is the default|
|`translate`|No|A list of further translations available for keys which match this rule|

The `translate` options apply to parts of the record under the bin level. Each translate item can have the following attributes:
|Field|Required|Use|
|-|-|-|
|`path`|Yes|Which sub-parts of the recrod to translate. See the section below on paths. This supports wildcards.
|`name`|No|What to rename this part of the path to.|
|`type`|No|The type of the item. This is `STRING` by default, but supports `INTEGER`, `DOUBLE`, `BOOLEAN`, `BYTES`, `LIST`, `MAP` too. The `LIST` and `MAP` options expect to the value passed from REDIS to be a JSON string which can be parsed into the appropriate types.

## Paths

 A path either denotes where to store data (in the `key` mapping), or which part of the object is affected by the `translate` option. Consider a key like `customer:1234:name` with a value of `Tim`. This could be captured as `customer:(\d+):name` with the `id` being `1234`, and the `path` is required to specify which bin (column) to store the data in. So the `path` might be set to `$.name` in this case. `$.` denotes the record, and uses JSON Path-like syntax. Alternatively, the `key` could be set to `customer:(\d+):(\w+)` with a `path` of `$.$2`, so the bin name is derived from the key as well.

 Paths can be nested arbitrarily deep and contain the following components:
 |Symbol|Meaning|
 |-|-|
 |`$.`|Matches the root of the record. All paths should start with this or a wildcard|
 |`name.`|Matches either the bin `name` or a map key of `name`|
 |`[1]`|Matches a list index of `1` (or whatever the argument is). List indexes are zero based and cannot be negative|
 |`*`|Matches exactly one path term. This may only be used in `translate` paths|
 |`**`|Matches zero or more path terms|
 
 ### Examples
1.  Import a hash from a key like `account:123`. Store this in the Account set. If there is a `balance` field in the hash, use this as a double, not a string:
 ```
 - key: account:(\d+)
  namespace: test
  set: Account
  id: $1
  type: INTEGER
  translate:
  - path: $.balance
    type: DOUBLE
```

2. Import a value similar to `customer:123:age` into the Customer set, with the last part of the path specifying the bin name. If the bin is `age` then store it as a double, if it is `heightInCm` then store it as a DOUBLE in a bin called `height`. If the bin is called `accounts` and it's a list, then store the list elements as integers instead of strings:
```
- key: customer:(\d+):(.+)
  namespace: test
  set: Customer
  id: $1
  type: INTEGER
  path: $.$2
  translate:
  - path: "$.age"
    type: INTEGER
  - path: "**.heightInCm"
    type: DOUBLE
    name: height
  - path: "$.accounts[*]"
    type: INTEGER
```
Note that if the requirement was that ALL lists were lists of numbers, then the last `path` could have been `**.[*]` instead of `$.accounts[*]`. And if the first item only in the list should be a number and the rest strings, it could have been `$.accounts[0]`

3. Import keys like `customer.1.addr.2.suburb` into the `cust` set into the `addr` bin, in the position with the second parameter (2nd location in this case), into a map with a key of `details` whose map key is the record id and the value of this is a map, with the key of `suburb`. (Yes, this is very contrived!)
```
- key: customer.(\d+).addr.(\d+).(\w+)
  namespace: test
  set: cust
  id: $1
  path: $.addr[$2].details.$1.$3
```
For example, `customer.1.addr.2.suburb` would produce:
```
aql> select * from test.cust
+-----+------------------------------------------------------------+
| PK  | addr                                                       |
+-----+------------------------------------------------------------+
| "1" | LIST('[NIL, NIL, {"details":{"1":{"suburb":"Chicago"}}}]') |
+-----+------------------------------------------------------------+
1 row in set (0.014 secs)
```