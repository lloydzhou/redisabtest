# redisab

```
AB.LAYER 获取所有的层，包含创建层的时间
AB.LAYER layer1 获取当前这一层的实验以及创建时间
AB.TEST test1 LAYER layer1 WEIGHT 100 NAME testname1 TYPE 创建一个实验，分配流量权重为100
AB.TEST test1 获取当前实验配置信息
AB.TEST test1 USER uid123 获取用户对应的版本
AB.VERSION test1 VALUE 1 WEIGHT 100 NAME version_name_1 设置某个版本
AB.VERSION test1 获取当前实验的所有版本
AB.VERSION 获取所有的版本
```




