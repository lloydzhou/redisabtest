# redisab

## command

### AB.TEST varname [NAME name] [LAYER layername] [WEIGHT weight] [TYPE string|number] [DEFAULT default_value]
> create one test, auto create layer and default version

### AB.TEST varname [USER userid]
> get version for current user

### AB.LAYER [layername]
> get all layers or get test for one layer by name

### AB.VERSION [varname] [VALUE version] [WEIGHT weight] [NAME version_name]
> if not have optional params, get all versions for system
> only have varname, get versions for one test by name
> give other optional params, set version for this test

### AB.TARGET [varname] [TARGET version]
> if not have optional params, get all targets for system
> only have varname, get targets for one test by name
> give other optional params, set target for this test

### alias

1. AB.VAR  --> AB.TEST
2. AB.VALUE  --> AB.VERSION

## example
```
AB.LAYER 获取所有的层，包含创建层的时间(get all layers)
AB.LAYER layer1 获取当前这一层的实验 (get test for current layer)
AB.TEST test1 LAYER layer1 WEIGHT 100 NAME testname1 TYPE  创建一个实验，分配流量权重为100 (create one test, auto create layer and default version)
AB.TEST test1 获取当前实验配置信息 (get info for current test)
AB.TEST test1 USER uid123 获取用户对应的版本 (get test version for user)
AB.VERSION test1 VALUE 1 WEIGHT 100 NAME version_name_1 设置某个版本 (set version)
AB.VERSION test1 获取当前实验的所有版本 (get info for current test)
AB.VERSION 获取所有的版本 (get all versions)
AB.TARGET test1 VALUE target1 设置指标 (set target for test)
AB.TARGET test1 获取test1所有指标 (get targets for test)
AB.TARGET 获取所有指标 (get all targets for system)
```

## TODO

-[x] AB.TARGET (add target for test)
-[ ] AB.TRACK (save user target)
-[ ] AB.RATE (calc uv + pv + target + min/max/mean/std)
-[ ] AB.TRAFIC (get pv and uv data by day)
-[ ] using timer to aggregate data for every task

