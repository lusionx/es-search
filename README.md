es-search
===============================================
simple tool for es query cli

```
  Usage: search [options]

  Options:

    -h, --help                 output usage information
    -V, --version              output the version number
    -d --database <i>          http://es.api.com/xx/yy/_search
    -q --query <json of mqes>  eg. {"a":1}
    -f --from [index]          from index number; disable when --scroll
    -s --size [number]         size number; the number of results per shard when --scroll
    --source [f1,f2...]        params _source
    --sort [field:desc/asc]    sort by field; disable when --scroll
    --scroll                   use scroll & echo with console.error, 使用2>>xx.log将输入重定向
    --delete [yes/Y]           delete query result
```
