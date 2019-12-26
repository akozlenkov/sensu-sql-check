# go-sensu-sql-check

### Overview

Sensu plugin for monitoring data via sql queries

### Help
```
Usage: go-sql-check [--driver] [--connection-url] --query --expression


Options:
  -d, --driver           One of clickhouse, presto (default "clickhouse")
  -c, --connection-url   DSN for connection (default "tcp://127.0.0.1:9000")
  -q, --query
  -e, --expression
```

### Example

```
go-sql-check \
-d clickhouse \
-c tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000 \
-q "select count(*) as some_var from some_table" \
-e "some_var != 0 ? info('exit with ret code 0', some_var+100/10) : error('exit with ret code 2', some_var-123)"
```
