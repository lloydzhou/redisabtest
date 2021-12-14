#include<time.h>
#include "./redismodule.h"
#include "./rmutil/util.h"
#include "./rmutil/strings.h"
#include "./rmutil/test_util.h"
#include "./murmurhash.c"

#define hash(key) (unsigned) murmurhash(key, (uint32_t) strlen(key), (uint32_t) atoi(seed));


int VersionCommandResponse(RedisModuleCtx *ctx, RedisModuleString *name, RedisModuleString *layer, RedisModuleString *type, RedisModuleString *value, unsigned hash) {

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "name");
  RedisModule_ReplyWithString(ctx, name);
  RedisModule_ReplyWithSimpleString(ctx, "layer");
  RedisModule_ReplyWithString(ctx, layer);
  RedisModule_ReplyWithSimpleString(ctx, "type");
  RedisModule_ReplyWithString(ctx, type);
  RedisModule_ReplyWithSimpleString(ctx, "value");
  if (RMUtil_StringEquals(type, RedisModule_CreateString(ctx, "number", strlen("number")))) {
    RedisModule_ReplyWithLongLong(ctx, (long long)atoi(RedisModule_StringPtrLen(value, NULL)));
  } else {
    RedisModule_ReplyWithString(ctx, value);
  }
  RedisModule_ReplyWithSimpleString(ctx, "hash");
  RedisModule_ReplyWithLongLong(ctx, hash);
  RedisModule_ReplySetArrayLength(ctx, 10);
  return REDISMODULE_OK;
}

int UserVersionCommand(RedisModuleCtx *ctx, RedisModuleString *test, RedisModuleString *user_id, long long ts) {

  RedisModuleString *test_key = RedisModule_CreateStringPrintf(ctx, "ab:var:%s", RedisModule_StringPtrLen(test, NULL));
  RedisModuleString *name = NULL;
  RedisModuleString *layer = NULL;
  RedisModuleString *type = NULL;
  RedisModuleString *default_value = NULL;
  long long weight = 0;

  RedisModuleCallReply *trep = RedisModule_Call(ctx, "HGETALL", "s", test_key);
  // RedisModule_Log(ctx, "warning", "hgetall %s", RedisModule_StringPtrLen(test_key, NULL));
  if (RedisModule_CallReplyType(trep) != REDISMODULE_REPLY_ARRAY) {
    return RedisModule_ReplyWithError(ctx, "not found test");
  }
  RedisModuleCallReply *rt = NULL;
  int i = 0;
  RedisModuleString *st = NULL;
  for (; i < RedisModule_CallReplyLength(trep); i += 2){
    rt = RedisModule_CallReplyArrayElement(trep, i);
    st = RedisModule_CreateStringFromCallReply(rt);
    // RedisModule_Log(ctx, "warning", "hget value %s", RedisModule_StringPtrLen(st, NULL));
    rt = RedisModule_CallReplyArrayElement(trep, i + 1);
    if (RMUtil_StringEquals(st, RedisModule_CreateString(ctx, "name", strlen("name")))) {
      name = RedisModule_CreateStringFromCallReply(rt);
    }
    if (RMUtil_StringEquals(st, RedisModule_CreateString(ctx, "layer", strlen("layer")))) {
      layer = RedisModule_CreateStringFromCallReply(rt);
    }
    if (RMUtil_StringEquals(st, RedisModule_CreateString(ctx, "type", strlen("type")))) {
      type = RedisModule_CreateStringFromCallReply(rt);
    }
    if (RMUtil_StringEquals(st, RedisModule_CreateString(ctx, "default", strlen("default")))) {
      default_value = RedisModule_CreateStringFromCallReply(rt);
    }
    if (RMUtil_StringEquals(st, RedisModule_CreateString(ctx, "weight", strlen("weight")))) {
      weight = (long long)atoi(RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(rt), NULL));
    }
    // RedisModule_Log(ctx, "warning", "hget value %s", RedisModule_StringPtrLen(st, NULL));
  }
  RedisModuleString *key = RedisModule_CreateStringPrintf(ctx, "%s:%s", RedisModule_StringPtrLen(test, NULL), RedisModule_StringPtrLen(user_id, NULL));
  char seed[] = "0";
  unsigned h = 0;
  h = hash(RedisModule_StringPtrLen(key, NULL));
  // RedisModule_Log(ctx, "warning", "hash %u", h);

  if (weight <= 0) {
    // layer weight <= 0
    return VersionCommandResponse(ctx, name, layer, type, default_value, h);
  }

  RedisModuleString *user_version = RedisModule_CreateStringPrintf(ctx, "ab:user:version:%s", RedisModule_StringPtrLen(test, NULL));

  RedisModuleString *value = NULL;
  RedisModuleCallReply *uvrep = RedisModule_Call(ctx, "HGET", "ss", user_version, user_id);
  RMUTIL_ASSERT_NOERROR(ctx, uvrep);
  if (RedisModule_CallReplyType(uvrep) != REDISMODULE_REPLY_STRING) {
    // RedisModule_Log(ctx, "warning", "user_version null");
    long long hash_weight = h % 10000;
    long long start_weight = 0;
    long long total_layer_weight = 0;

    RedisModuleString *layer_key = RedisModule_CreateStringPrintf(ctx, "ab:layer:%s", RedisModule_StringPtrLen(layer, NULL));
    RedisModuleCallReply *lwrep = RedisModule_Call(ctx, "SORT", "scccccc", layer_key, "BY", "ab:var:*->created", "GET", "#", "GET", "ab:var:*->weight");
    // RedisModule_Log(ctx, "warning", "sort %s", RedisModule_StringPtrLen(layer_key, NULL));
    if (RedisModule_CallReplyType(lwrep) != REDISMODULE_REPLY_ARRAY) {
      return RedisModule_ReplyWithError(ctx, "not found test belong layer");
    }
    RedisModuleCallReply *rlt = NULL;
    int j = 0;
    long long t = 0;
    RedisModuleString *slt = NULL;
    for (j = 0; j < RedisModule_CallReplyLength(lwrep); j += 2){
      rlt = RedisModule_CallReplyArrayElement(lwrep, j + 1);
      slt = RedisModule_CreateStringFromCallReply(rlt);
      // RedisModule_Log(ctx, "warning", "total_layer_weight %lld %s", total_layer_weight, RedisModule_StringPtrLen(slt, NULL));
      total_layer_weight += (long long)atoi(RedisModule_StringPtrLen(slt, NULL));
      // RedisModule_Log(ctx, "warning", "total_layer_weight %lld", total_layer_weight);
    }
    for (j = 0; j < RedisModule_CallReplyLength(lwrep); j += 2){
      rlt = RedisModule_CallReplyArrayElement(lwrep, j);
      slt = RedisModule_CreateStringFromCallReply(rlt);
      // test var == current var break
      if (RMUtil_StringEquals(name, RedisModule_CreateStringFromCallReply(rlt))) {
        break;
      }

      rlt = RedisModule_CallReplyArrayElement(trep, j + 1);
      t = (long long)atoi(RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(rlt), NULL));
      start_weight = start_weight + (long long)(1.0 * t / total_layer_weight * 10000);
      // RedisModule_Log(ctx, "warning", "start_weight %lld %lld %lld", start_weight, t, total_layer_weight);
    }
    // RedisModule_Log(ctx, "warning", "start_weight %lld hash_weight %lld layer_weight %lld total_layer_weight %lld", start_weight, hash_weight, weight, total_layer_weight);
    if (
      (start_weight < hash_weight && hash_weight <= start_weight + (long long)(1.0 * weight / total_layer_weight * 10000))
      || (0 == start_weight && 0 == hash_weight)
    ) {
      // calc versions
      // RedisModule_Log(ctx, "warning", "calc version %lld %lld %lld", start_weight, t, total_layer_weight);
      RedisModuleString *version_key = RedisModule_CreateStringPrintf(ctx, "ab:version:%s", RedisModule_StringPtrLen(test, NULL));
      RedisModuleCallReply *vrep = RedisModule_Call(ctx, "SORT", "scccccc", version_key, "BY", "*->created", "GET", "*->value", "GET", "*->weight");
      // RedisModule_Log(ctx, "warning", "sort %s", RedisModule_StringPtrLen(version_key, NULL));
      if (RedisModule_CallReplyType(vrep) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(ctx, "not found version belong test");
      }
      RedisModuleCallReply *rvt = NULL;
      int k = 0;
      long long n = 0;
      long long total_test_weight = 0;
      long long real_weight = start_weight;
      RedisModuleString *svt = NULL;
      for (k = 0; k < RedisModule_CallReplyLength(vrep); k += 2){
        rvt = RedisModule_CallReplyArrayElement(vrep, k + 1);
        svt = RedisModule_CreateStringFromCallReply(rvt);
        // RedisModule_Log(ctx, "warning", "total_test_weight %lld %s", total_test_weight, RedisModule_StringPtrLen(svt, NULL));
        total_test_weight += (long long)atoi(RedisModule_StringPtrLen(svt, NULL));
        // RedisModule_Log(ctx, "warning", "total_test_weight %lld", total_test_weight);
      }
      // RedisModule_Log(ctx, "warning", "total_test_weight %lld", total_test_weight);
      for (k = 0; k < RedisModule_CallReplyLength(vrep); k += 2){
        rvt = RedisModule_CallReplyArrayElement(vrep, k + 1);
        n = (long long)atoi(RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(rvt), NULL));
        if (n > 0) {
          // RedisModule_Log(ctx, "warning", "real_weight %lld %lld %lld", real_weight, n, total_test_weight);
          n = (long long)(1.0 * n / total_test_weight * 100);
          real_weight = real_weight + (int)(1.0 * n * weight / total_layer_weight * 100);
          // RedisModule_Log(ctx, "warning", "real_weight %lld %lld %lld", real_weight, n, total_test_weight);
          if (hash_weight <= real_weight) {
            rvt = RedisModule_CallReplyArrayElement(vrep, k);
            value = RedisModule_CreateStringFromCallReply(rvt);
            // set user:version
            // RedisModule_Log(ctx, "warning", "hset user version %s %s", RedisModule_StringPtrLen(user_id, NULL), RedisModule_StringPtrLen(value, NULL));
            RedisModule_Call(ctx, "HSET", "sss", user_version, user_id, value);
            break;
          }
        }
        // RedisModule_Log(ctx, "warning", "hget value %s", RedisModule_StringPtrLen(st, NULL));
      }
    } else {
      // in layer but not in test
      return VersionCommandResponse(ctx, name, layer, type, value, h);
    }
  } else {
    // get user version from cash
    value = RedisModule_CreateStringFromCallReply(uvrep);
    // RedisModule_Log(ctx, "warning", "user_version %s", RedisModule_StringPtrLen(value, NULL));
  }
  if (!value) {
    value = default_value ? default_value : RedisModule_CreateString(ctx, "0", strlen("0"));
  }

  RedisModuleString *uv_key = RedisModule_CreateStringPrintf(ctx, "ab:uv:%s:%s", RedisModule_StringPtrLen(test, NULL), RedisModule_StringPtrLen(value, NULL));
  // pv inc by user_id
  RMUTIL_ASSERT_NOERROR(ctx, RedisModule_Call(ctx, "ZINCRBY", "sls", uv_key, 1, user_id));

  time_t _ts = ts / 1000;
  struct tm tm = *localtime(&_ts);
  char day[100];
  strftime(day, sizeof(day)-1, "%Y-%m-%d", &tm);
  // RedisModule_Log(ctx, "warning", "day %s", day);

  RedisModuleString *days_key = RedisModule_CreateStringPrintf(ctx, "ab:days:%s", RedisModule_StringPtrLen(test, NULL));
  RMUTIL_ASSERT_NOERROR(ctx, RedisModule_Call(ctx, "SADD", "sc", days_key, day));
  RedisModuleString *day_key = RedisModule_CreateStringPrintf(ctx, "ab:day:%s", day);
  RedisModuleString *day_value = RedisModule_CreateStringPrintf(ctx, "%s:%s:pv", RedisModule_StringPtrLen(test, NULL), RedisModule_StringPtrLen(value, NULL));
  RMUTIL_ASSERT_NOERROR(ctx, RedisModule_Call(ctx, "HINCRBY", "ssl", day_key, day_value, 1));

  // RedisModule_Log(ctx, "warning", "user_version %s", RedisModule_StringPtrLen(value, NULL));
  return VersionCommandResponse(ctx, name, layer, type, value, h);
}

/*
 * AB.TARGET <testname> [TARGET <target>]
*/
int TargetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // RedisModule_Log(ctx, "warning", "argc %d", argc);

  if (argc < 1 ) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 1) {
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "cccccccccccccccc",
        "ab:targets", "BY", "*->created",
        "GET", "#", "GET", "*->name", "GET", "*->test",
        "GET", "*->value", "GET", "*->created", "GET", "*->updated", "DESC"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithSimpleString(ctx, "header");

      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      RedisModule_ReplyWithSimpleString(ctx, "target");
      RedisModule_ReplyWithSimpleString(ctx, "name");
      RedisModule_ReplyWithSimpleString(ctx, "test");
      RedisModule_ReplyWithSimpleString(ctx, "value");
      RedisModule_ReplyWithSimpleString(ctx, "created");
      RedisModule_ReplyWithSimpleString(ctx, "updated");
      RedisModule_ReplySetArrayLength(ctx, 6);

      RedisModule_ReplyWithSimpleString(ctx, "body");
      RedisModule_ReplyWithCallReply(ctx, arep);
    RedisModule_ReplySetArrayLength(ctx, 4);
    return REDISMODULE_OK;
    // return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModule_AutoMemory(ctx);

  RedisModuleString *test_key = RedisModule_CreateStringPrintf(ctx, "ab:target:%s", RedisModule_StringPtrLen(argv[1], NULL));
  if (argc == 2) {
    // SORT ab:version:test1 BY ab:version:*->updated get # get ab:version:*->name get ab:version:*->weight get ab:version:*->value
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "sccccccccccccccc",
        test_key, "BY", "*->created",
        "GET", "#", "GET", "*->name", "GET", "*->test",
        "GET", "*->value", "GET", "*->created", "GET", "*->updated", "DESC"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithSimpleString(ctx, "header");

      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      RedisModule_ReplyWithSimpleString(ctx, "target");
      RedisModule_ReplyWithSimpleString(ctx, "name");
      RedisModule_ReplyWithSimpleString(ctx, "test");
      RedisModule_ReplyWithSimpleString(ctx, "value");
      RedisModule_ReplyWithSimpleString(ctx, "created");
      RedisModule_ReplyWithSimpleString(ctx, "updated");
      RedisModule_ReplySetArrayLength(ctx, 6);

      RedisModule_ReplyWithSimpleString(ctx, "body");
      RedisModule_ReplyWithCallReply(ctx, arep);
    RedisModule_ReplySetArrayLength(ctx, 4);
    return REDISMODULE_OK;
    // return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModuleString *value = NULL;
  RMUtil_ParseArgsAfter("VALUE", argv, argc, "s", &value);
  if (value == NULL) {
    RMUtil_ParseArgsAfter("TARGET", argv, argc, "s", &value);
  }
  if (value == NULL && argc == 3) {
    value = argv[2];
  }
  if (value == NULL) {
    RedisModule_Log(ctx, "warning", "No target found");
    return RedisModule_ReplyWithError(ctx, "NEED VALUE");
  }
  RedisModuleString *target = RedisModule_CreateStringPrintf(ctx, "ab:target:%s:%s", RedisModule_StringPtrLen(argv[1], NULL), RedisModule_StringPtrLen(value, NULL));
  // RedisModule_Log(ctx, "warning", "target %s", RedisModule_StringPtrLen(target, NULL));

  long long ts = RedisModule_Milliseconds();

  RedisModuleString *name = NULL;
  RMUtil_ParseArgsAfter("NAME", argv, argc, "s", &name);

  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "HSET", "scscsclcs", target, "name", name ? name : target, "test", argv[1], "updated", ts, "value", value);
  RMUTIL_ASSERT_NOERROR(ctx, rep);
  RedisModule_Call(ctx, "HSETNX", "scl", target, "created", ts);

  RedisModuleCallReply *srep =
      RedisModule_Call(ctx, "ZADD", "ccls", "ab:targets", "NX", ts, target);
  RMUTIL_ASSERT_NOERROR(ctx, srep);

  RedisModuleCallReply *lzrep =
      RedisModule_Call(ctx, "ZADD", "scls", test_key, "NX", ts, target);
  RMUTIL_ASSERT_NOERROR(ctx, lzrep);

  // if the value was null before - we just return null
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  // forward the reply to the client
  RedisModule_ReplyWithCallReply(ctx, rep);
  return REDISMODULE_OK;
}


/*
 * AB.VERSION <testname> [VALUE <version>] [WEIGHT <weight>] [NAME <version_name>]
*/
int VersionCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // RedisModule_Log(ctx, "warning", "argc %d", argc);

  if (argc < 1 ) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 1) {
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "cccccccccccccccccc",
        "ab:versions", "BY", "*->created",
        "GET", "#", "GET", "*->name", "GET", "*->test", "GET", "*->value",
        "GET", "*->weight", "GET", "*->created", "GET", "*->updated", "DESC"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithSimpleString(ctx, "header");

      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      RedisModule_ReplyWithSimpleString(ctx, "version");
      RedisModule_ReplyWithSimpleString(ctx, "name");
      RedisModule_ReplyWithSimpleString(ctx, "test");
      RedisModule_ReplyWithSimpleString(ctx, "value");
      RedisModule_ReplyWithSimpleString(ctx, "weight");
      RedisModule_ReplyWithSimpleString(ctx, "created");
      RedisModule_ReplyWithSimpleString(ctx, "updated");
      RedisModule_ReplySetArrayLength(ctx, 7);

      RedisModule_ReplyWithSimpleString(ctx, "body");
      RedisModule_ReplyWithCallReply(ctx, arep);
    RedisModule_ReplySetArrayLength(ctx, 4);
    return REDISMODULE_OK;
    // return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModule_AutoMemory(ctx);

  RedisModuleString *test_key = RedisModule_CreateStringPrintf(ctx, "ab:version:%s", RedisModule_StringPtrLen(argv[1], NULL));
  if (argc == 2) {
    // SORT ab:version:test1 BY ab:version:*->updated get # get ab:version:*->name get ab:version:*->weight get ab:version:*->value
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "sccccccccccccccccc",
        test_key, "BY", "*->created",
        "GET", "#", "GET", "*->name", "GET", "*->test", "GET", "*->value",
        "GET", "*->weight", "GET", "*->created", "GET", "*->updated", "DESC"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithSimpleString(ctx, "header");

      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      RedisModule_ReplyWithSimpleString(ctx, "version");
      RedisModule_ReplyWithSimpleString(ctx, "name");
      RedisModule_ReplyWithSimpleString(ctx, "test");
      RedisModule_ReplyWithSimpleString(ctx, "value");
      RedisModule_ReplyWithSimpleString(ctx, "weight");
      RedisModule_ReplyWithSimpleString(ctx, "created");
      RedisModule_ReplyWithSimpleString(ctx, "updated");
      RedisModule_ReplySetArrayLength(ctx, 7);

      RedisModule_ReplyWithSimpleString(ctx, "body");
      RedisModule_ReplyWithCallReply(ctx, arep);
    RedisModule_ReplySetArrayLength(ctx, 4);
    return REDISMODULE_OK;
    // return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModuleString *value = NULL;
  RMUtil_ParseArgsAfter("VALUE", argv, argc, "s", &value);
  if (value == NULL && argc == 3) {
    value = argv[2];
  }
  if (value == NULL) {
    RedisModule_Log(ctx, "warning", "No version found");
    return RedisModule_ReplyWithError(ctx, "NEED VALUE");
  }
  RedisModuleString *version = RedisModule_CreateStringPrintf(ctx, "ab:version:%s:%s", RedisModule_StringPtrLen(argv[1], NULL), RedisModule_StringPtrLen(value, NULL));
  // RedisModule_Log(ctx, "warning", "version %s", RedisModule_StringPtrLen(version, NULL));

  long long ts = RedisModule_Milliseconds();

  RedisModuleString *name = NULL;
  RMUtil_ParseArgsAfter("NAME", argv, argc, "s", &name);

  long long weight = 100;
  RMUtil_ParseArgsAfter("WEIGHT", argv, argc, "l", &weight);
  // RedisModule_Log(ctx, "warning", "weight %lld", weight);

  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "HSET", "scscsclclcs", version, "name", name ? name : version, "test", argv[1], "weight", weight, "updated", ts, "value", value);
  RMUTIL_ASSERT_NOERROR(ctx, rep);
  RedisModule_Call(ctx, "HSETNX", "scl", version, "created", ts);

  RedisModuleCallReply *srep =
      RedisModule_Call(ctx, "ZADD", "ccls", "ab:versions", "NX", ts, version);
  RMUTIL_ASSERT_NOERROR(ctx, srep);

  RedisModuleCallReply *lzrep =
      RedisModule_Call(ctx, "ZADD", "scls", test_key, "NX", ts, version);
  RMUTIL_ASSERT_NOERROR(ctx, lzrep);

  // if the value was null before - we just return null
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  // forward the reply to the client
  RedisModule_ReplyWithCallReply(ctx, rep);
  return REDISMODULE_OK;
}


/*
 * AB.TEST <varname> [NAME <name>] [LAYER <layername>] [WEIGHT <weight>] [TYPE string|number] [DEFAULT <default_value>]
 * AB.TEST <varname> [USER <user_id>]
*/
int TestCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // RedisModule_Log(ctx, "warning", "argc %d", argc);

  if (argc < 1 ) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 1) {
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "cccccccccccccccc",
        "ab:tests", "BY", "ab:var:*->created",
        "GET", "#", "GET", "ab:var:*->name", "GET", "ab:var:*->layer",
        "GET", "ab:var:*->weight", "GET", "ab:var:*->created", "GET", "ab:var:*->updated", "DESC"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithSimpleString(ctx, "header");

      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      RedisModule_ReplyWithSimpleString(ctx, "test");
      RedisModule_ReplyWithSimpleString(ctx, "name");
      RedisModule_ReplyWithSimpleString(ctx, "layer");
      RedisModule_ReplyWithSimpleString(ctx, "weight");
      RedisModule_ReplyWithSimpleString(ctx, "created");
      RedisModule_ReplyWithSimpleString(ctx, "updated");
      RedisModule_ReplySetArrayLength(ctx, 6);

      RedisModule_ReplyWithSimpleString(ctx, "body");
      RedisModule_ReplyWithCallReply(ctx, arep);
    RedisModule_ReplySetArrayLength(ctx, 4);
    return REDISMODULE_OK;
    // return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModule_AutoMemory(ctx);

  RedisModuleString *var = RedisModule_CreateStringPrintf(ctx, "ab:var:%s", RedisModule_StringPtrLen(argv[1], NULL));
  // RedisModule_Log(ctx, "warning", "timestamp %lld var %s", ts, RedisModule_StringPtrLen(var, NULL));

  if (argc == 2) {
    RedisModuleCallReply *arep =
        RedisModule_Call(ctx, "HGETALL", "s", var);
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  long long ts = RedisModule_Milliseconds();

  RedisModuleString *user = NULL;
  RMUtil_ParseArgsAfter("USER", argv, argc, "s", &user);
  if (user != NULL) {
    // RedisModule_Log(ctx, "warning", "user %s", RedisModule_StringPtrLen(user, NULL));
    return UserVersionCommand(ctx, argv[1], user, ts);
  }

  RedisModuleString *layer = NULL;
  RMUtil_ParseArgsAfter("LAYER", argv, argc, "s", &layer);
  if (layer == NULL) {
    RedisModule_Log(ctx, "warning", "No layer found");
    return RedisModule_ReplyWithError(ctx, "NEED LAYER");
  }

  RedisModuleString *name = NULL;
  RMUtil_ParseArgsAfter("NAME", argv, argc, "s", &name);

  char default_type[16] = "number";
  RedisModuleString *type = NULL;
  RMUtil_ParseArgsAfter("TYPE", argv, argc, "s", &type);

  // validate type
  if (type != NULL) {
    if (
      !RMUtil_StringEquals(type, RedisModule_CreateString(ctx, default_type, strlen(default_type))) &&
      !RMUtil_StringEquals(type, RedisModule_CreateString(ctx, "string", strlen("string")))
    ) {
      type = NULL;
    }
  }
  if (type == NULL) {
    RedisModuleCallReply *typerep = RedisModule_Call(ctx, "HGET", "sc", var, "type");
    RMUTIL_ASSERT_NOERROR(ctx, typerep);
    // RedisModule_Log(ctx, "warning", "type %s", RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(typerep), NULL));
    if (RedisModule_CallReplyType(typerep) == REDISMODULE_REPLY_STRING) {
      type = RedisModule_CreateStringFromCallReply(typerep);
    } else {
      type = RedisModule_CreateString(ctx, default_type, strlen(default_type));
    }
  }
  // RedisModule_Log(ctx, "warning", "default_type %s", RedisModule_StringPtrLen(type, NULL));

  RedisModuleString *default_value = NULL;
  RMUtil_ParseArgsAfter("DEFAULT", argv, argc, "s", &default_value);
  if (default_value == NULL){
    RedisModuleCallReply *drep = RedisModule_Call(ctx, "HGET", "sc", var, "default");
    RMUTIL_ASSERT_NOERROR(ctx, drep);
    if (RedisModule_CallReplyType(drep) == REDISMODULE_REPLY_STRING) {
      default_value = RedisModule_CreateStringFromCallReply(drep);
    } else {
      default_value = RedisModule_CreateStringFromLongLong(ctx, 0);
    }
  }
  // RedisModule_Log(ctx, "warning", "default_value %s", RedisModule_StringPtrLen(default_value, NULL));

  long long weight = 100;
  RMUtil_ParseArgsAfter("WEIGHT", argv, argc, "l", &weight);
  // RedisModule_Log(ctx, "warning", "weight %lld", weight);

  RedisModuleCallReply *rep =
    RedisModule_Call(ctx, "HSET", "scscsclclcscs", var, "name", name ? name : var, "layer", layer, "weight", weight, "updated", ts, "type", type, "default", default_value);
  RMUTIL_ASSERT_NOERROR(ctx, rep);
  RedisModule_Call(ctx, "HSETNX", "scl", var, "created", ts);

  RedisModuleCallReply *dvrep =
      RedisModule_Call(ctx, "AB.VERSION", "scscl", argv[1], "VALUE", default_value, "WEIGHT", 100);
  RMUTIL_ASSERT_NOERROR(ctx, dvrep);

  RedisModuleCallReply *srep =
      RedisModule_Call(ctx, "ZADD", "ccls", "ab:tests", "NX", ts, argv[1]);
  RMUTIL_ASSERT_NOERROR(ctx, srep);

  RedisModuleString *layer_key = RedisModule_CreateStringPrintf(ctx, "ab:layer:%s", RedisModule_StringPtrLen(layer, NULL));
  // RedisModule_Log(ctx, "warning", "layer_key %s", RedisModule_StringPtrLen(layer_key, NULL));
  RedisModuleCallReply *lzrep =
      RedisModule_Call(ctx, "ZADD", "scls", layer_key, "NX", ts, argv[1]);
  RMUTIL_ASSERT_NOERROR(ctx, lzrep);

  RedisModuleCallReply *lrep =
      RedisModule_Call(ctx, "ZADD", "ccls", "ab:layers", "NX", ts, layer);
  RMUTIL_ASSERT_NOERROR(ctx, lrep);

  // if the value was null before - we just return null
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  // forward the reply to the client
  RedisModule_ReplyWithCallReply(ctx, rep);
  return REDISMODULE_OK;
}


/*
 * AB.LAYER [<layername>]
*/
int LayerCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // RedisModule_Log(ctx, "warning", "argc %d", argc);
  if (argc < 1 || argc > 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 1) {
    // ZRANGE ab:layers -inf inf WITHSCORES
    RedisModuleCallReply *arep =
        RedisModule_Call(ctx, "ZRANGE", "ccccc", "ab:layers", "-inf", "inf", "BYSCORE", "WITHSCORES");
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
  } 

  RedisModule_AutoMemory(ctx);

  if (argc == 2) {
    RedisModuleString *layer_key = RedisModule_CreateStringPrintf(ctx, "ab:layer:%s", RedisModule_StringPtrLen(argv[1], NULL));

    RedisModuleCallReply *arep =
      RedisModule_Call(ctx, "ZRANGE", "scccc", layer_key, "-inf", "inf", "BYSCORE", "WITHSCORES");
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  return REDISMODULE_ERR;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

  // Register the module itself
  if (RedisModule_Init(ctx, "ab", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // register ab.layer - the default registration syntax
  if (RedisModule_CreateCommand(ctx, "ab.layer", LayerCommand, "readonly",
                                1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // register ab.test - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.test", TestCommand);

  // register ab.var - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.var", TestCommand);

  // register ab.version - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.version", VersionCommand);

  // register ab.value - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.value", VersionCommand);

  // register ab.target - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.target", TargetCommand);

  return REDISMODULE_OK;
}
