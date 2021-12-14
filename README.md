# redisab

## command
```
AB.LAYER 获取所有的层，包含创建层的时间(get all layers)
AB.LAYER layer1 获取当前这一层的实验 (get test for current layer)
AB.TEST test1 LAYER layer1 WEIGHT 100 NAME testname1 TYPE  创建一个实验，分配流量权重为100 (create one test, auto create layer and default version)
AB.TEST test1 获取当前实验配置信息 (get info for current test)
AB.TEST test1 USER uid123 获取用户对应的版本 (get test version for user)
AB.VERSION test1 VALUE 1 WEIGHT 100 NAME version_name_1 设置某个版本 (set version)
AB.VERSION test1 获取当前实验的所有版本 (get info for current test)
AB.VERSION 获取所有的版本 (get all versions)
```

## TODO

1. AB.TARGET (add target for test)
2. AB.TRACK (save user target)
3. AB.RATE (calc uv + pv + target + min/max/mean/std)
4. AB.TRAFIC (get pv and uv data by day)


