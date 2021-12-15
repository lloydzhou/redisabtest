#define REDISMODULE_EXPERIMENTAL_API
#include <time.h>
#include <math.h>
#include "./redismodule.h"
#include "./rmutil/util.h"
#include "./rmutil/strings.h"
#include "./rmutil/test_util.h"
#include "./rmutil/periodic.c"
#include "./murmurhash.c"


#define hash(key) (unsigned) murmurhash(key, (uint32_t) strlen(key), (uint32_t) atoi(seed));

#define C2S(ctx, s) RedisModule_CreateString(ctx, s, strlen(s))

static struct RMUtilTimer *interval_timer;


int ListResponse(RedisModuleCtx *ctx, const char *fmt, ...) {
  va_list argv;
  int i = 0;
  // int argc = 0;
  RedisModuleString *s;
  char *c;
  long long l;
  unsigned u;
  va_start(argv, fmt);
  // RedisModule_Log(ctx, "warning", "fmt %s", fmt);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while(*fmt) {
    // RedisModule_Log(ctx, "warning", "fmt %d %s", i, fmt);
    switch(*fmt) {
      case 's':
        s = va_arg(argv, RedisModuleString*);
        RedisModule_ReplyWithString(ctx, s);
        // RedisModule_Log(ctx, "warning", "argv %d %s", i, RedisModule_StringPtrLen(s, NULL));
        break;
      case 'c':
        c = va_arg(argv, char*);
        RedisModule_ReplyWithSimpleString(ctx, c);
        // RedisModule_Log(ctx, "warning", "argv %d %s", i, c);
        break;
      case 'l':
        l = va_arg(argv, long long);
        RedisModule_ReplyWithLongLong(ctx, l);
        // RedisModule_Log(ctx, "warning", "argv %d %lld", i, l);
        break;
      case 'u':
        u = va_arg(argv, unsigned);
        RedisModule_ReplyWithLongLong(ctx, u);
        // RedisModule_Log(ctx, "warning", "argv %d %u", i, u);
        break;
      default:
        break;
    }
    fmt++;
    i++;
  }
  RedisModule_ReplySetArrayLength(ctx, i);
  // RedisModule_Log(ctx, "warning", "argc %d", i);
  va_end(argv);
  return REDISMODULE_OK;
}

int VersionCommandResponse(RedisModuleCtx *ctx, RedisModuleString *name, RedisModuleString *layer, RedisModuleString *type, RedisModuleString *value, unsigned hash) {

  if (RMUtil_StringEquals(type, RedisModule_CreateString(ctx, "number", strlen("number")))) {
    return ListResponse(ctx, "cscscsclcu", "name", name, "layer", layer, "type", type, "value", (long long)atoi(RedisModule_StringPtrLen(value, NULL)), "hash", hash);
  } else {
    return ListResponse(ctx, "cscscscscu", "name", name, "layer", layer, "type", type, "value", value, "hash", hash);
  }
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
  if (!layer || !type || !weight || !default_value) {
    return RedisModule_ReplyWithError(ctx, "not valid test");
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

  RedisModuleString *uv_key = RedisModule_CreateStringPrintf(ctx, "ab:version:%s:%s:uv", RedisModule_StringPtrLen(test, NULL), RedisModule_StringPtrLen(value, NULL));
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
      ListResponse(ctx, "cccccc", "target", "name", "test", "value", "created", "updated");
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
      ListResponse(ctx, "cccccc", "target", "name", "test", "value", "created", "updated");
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
      ListResponse(ctx, "ccccccc", "version", "name", "test", "value", "weight", "created", "updated");
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
      ListResponse(ctx, "ccccccc", "version", "name", "test", "value", "weight", "created", "updated");
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
      ListResponse(ctx, "cccccc", "test", "name", "layer", "weight", "created", "updated");
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
 * AB.TRACK <user_id> [target inc] [target inc] [target inc]
*/
int TrackCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // RedisModule_Log(ctx, "warning", "argc %d", argc);
  if (argc < 4 || argc % 2 == 1) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  RedisModuleString *user_id = argv[1];

  RedisModuleCallReply *trep = RedisModule_Call(ctx, "sort", "ccccccccc", "ab:targets", "by", "*", "get", "#", "get", "*->test", "get", "*->value");

  time_t _ts = RedisModule_Milliseconds() / 1000;
  struct tm tm = *localtime(&_ts);
  char day[100];
  strftime(day, sizeof(day)-1, "%Y-%m-%d", &tm);
  // RedisModule_Log(ctx, "warning", "day %s", day);

  int i, j;
  for (i = 2; i < argc; i += 2) {
    RedisModuleString *target = argv[i];
    long long inc = (long long)atoi(RedisModule_StringPtrLen(argv[i + 1], NULL));
    // RedisModule_Log(ctx, "warning", "target %s %s %lld", RedisModule_StringPtrLen(user_id, NULL), RedisModule_StringPtrLen(target, NULL), inc);
    if (inc > 0) {
      for (j = 0; j < RedisModule_CallReplyLength(trep); j += 3) {
        RedisModuleCallReply *t = RedisModule_CallReplyArrayElement(trep, j+1);
        RedisModuleString *ttest = RedisModule_CreateStringFromCallReply(t);
        // RedisModule_Log(ctx, "warning", "target test name %s", RedisModule_StringPtrLen(ttest, NULL));
        RedisModuleString *ttarget = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(trep, j+2));
        // RedisModule_Log(ctx, "warning", "target %s ttarget %s", RedisModule_StringPtrLen(target, NULL), RedisModule_StringPtrLen(ttarget, NULL));
        if (RMUtil_StringEquals(target, ttarget)) {
          // get value by user_id and test
          RedisModuleString *user_version = RedisModule_CreateStringPrintf(ctx, "ab:user:version:%s", RedisModule_StringPtrLen(ttest, NULL));
          RedisModuleCallReply *value_rep = RedisModule_Call(ctx, "HGET", "ss", user_version, user_id);
          if (RedisModule_CallReplyType(value_rep) == REDISMODULE_REPLY_STRING) {
            RedisModuleString *value = RedisModule_CreateStringFromCallReply(value_rep);
            // RedisModule_Log(ctx, "warning", "target value %s", RedisModule_StringPtrLen(value, NULL));
            // start to store target inc
            // red:sadd("days:" .. var_name, today)
            RedisModuleString *days_key = RedisModule_CreateStringPrintf(ctx, "ab:days:%s", RedisModule_StringPtrLen(ttest, NULL));
            RMUTIL_ASSERT_NOERROR(ctx, RedisModule_Call(ctx, "SADD", "sc", days_key, day));
            // red:hincrby("day:" .. today, key .. ":pv", 1)
            RedisModuleString *key = RedisModule_CreateStringPrintf(ctx, "%s:%s:%s", RedisModule_StringPtrLen(ttest, NULL), RedisModule_StringPtrLen(value, NULL), RedisModule_StringPtrLen(target, NULL));
            RedisModuleString *day_key = RedisModule_CreateStringPrintf(ctx, "ab:day:%s", day);
            RedisModuleString *day_value = RedisModule_CreateStringPrintf(ctx, "%s:pv", RedisModule_StringPtrLen(key, NULL));
            RMUTIL_ASSERT_NOERROR(ctx, RedisModule_Call(ctx, "HINCRBY", "ssl", day_key, day_value, 1));
            // track_key = ab:version:<test>:<value>:<target>:track
            RedisModuleString *track_key = RedisModule_CreateStringPrintf(
              ctx, "ab:version:%s:track", RedisModule_StringPtrLen(key, NULL)
            );
            // red:zincrby("track:" .. key, inc, user_id)
            RMUTIL_ASSERT_NOERROR(ctx, RedisModule_Call(ctx, "ZINCRBY", "sls", track_key, 1, user_id));
          }
        }
      }
    }
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
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

/*
 * AB.RATE testname
*/
int RateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);
  RedisModuleString *test = argv[1];

  RedisModuleString *version_key = RedisModule_CreateStringPrintf(ctx, "ab:version:%s", RedisModule_StringPtrLen(test, NULL));
  RedisModuleCallReply *versions = RedisModule_Call(ctx, "SORT", "scccc", version_key, "BY", "*->created", "GET", "*->value");
  RMUTIL_ASSERT_NOERROR(ctx, versions);

  RedisModuleString *target_key = RedisModule_CreateStringPrintf(ctx, "ab:target:%s", RedisModule_StringPtrLen(test, NULL));
  RedisModuleCallReply *targets = RedisModule_Call(ctx, "SORT", "scccc", target_key, "BY", "*->created", "GET", "*->value");
  RMUTIL_ASSERT_NOERROR(ctx, targets);

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "targets");
  RedisModule_ReplyWithCallReply(ctx, targets);
  int i = 0;
  for (i = 0; i < RedisModule_CallReplyLength(versions); i += 2) {
    RedisModuleString *value = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(versions, i + 1));
    RedisModule_ReplyWithSimpleString(ctx, "version");
    RedisModule_ReplyWithString(ctx, value);

    RedisModuleString *key = RedisModule_CreateStringPrintf(ctx, "ab:version:%s:%s", RedisModule_StringPtrLen(test, NULL), RedisModule_StringPtrLen(value, NULL));
    // RedisModule_Log(ctx, "warning", "HGETALL %s", RedisModule_StringPtrLen(key, NULL));
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "HGETALL", "s", key);
    RMUTIL_ASSERT_NOERROR(ctx, rep);
    RedisModule_ReplyWithSimpleString(ctx, "data");
    RedisModule_ReplyWithCallReply(ctx, rep);
  }
  RedisModule_ReplySetArrayLength(ctx, 2 + i * 2);

  return REDISMODULE_OK;
}

/*
 * AB.TRAFFIC testname
*/
int TrafficCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);
  RedisModuleString *test = argv[1];

  size_t MAX = 10000;

  RedisModuleString *days_key = RedisModule_CreateStringPrintf(ctx, "ab:days:%s", RedisModule_StringPtrLen(test, NULL));
  RedisModuleCallReply *days_rep = RedisModule_Call(ctx, "SORT", "scccllcc", days_key, "BY", "*", "LIMIT", 0, MAX, "GET", "#");
  RMUTIL_ASSERT_NOERROR(ctx, days_rep);

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int i;
  for (i = 0; i < RedisModule_CallReplyLength(days_rep); i++) {
    RedisModuleString *day = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(days_rep, i));
    RedisModule_ReplyWithString(ctx, day);

    RedisModuleString *day_key = RedisModule_CreateStringPrintf(ctx, "ab:day:%s", RedisModule_StringPtrLen(day, NULL));
    RedisModuleString *match = RedisModule_CreateStringPrintf(ctx, "%s*", RedisModule_StringPtrLen(test, NULL));
    // TODO size(version) * size(target) < 1000
    // RedisModule_Log(ctx, "warning", "HSCAN %s", RedisModule_StringPtrLen(day_key, NULL));
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "HSCAN", "sccscl", day_key, "0", "MATCH", match, "COUNT", MAX);
    RedisModule_ReplyWithCallReply(ctx, RedisModule_CallReplyArrayElement(rep, 1));
  }
  RedisModule_ReplySetArrayLength(ctx, i * 2);

  return REDISMODULE_OK;
}

/*
 * AB.TIMER [timeout]
*/
int TimerCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  // read timeout
  mstime_t t = (mstime_t)atoi(RedisModule_StringPtrLen(argv[1], NULL));
  // interval > 3s
  if (t >= 3) {
    RMUtilTimer_SetInterval(interval_timer, (struct timespec){.tv_sec = 0, .tv_nsec = t * 1000000000 });
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  return RedisModule_WrongArity(ctx);

}

void aggregate(RedisModuleCtx *ctx, RedisModuleString *key, long long *count, long long *sum, long long *min, long long *max, double *mean, double *std) {
  *count = *sum = *min = *max = 0;
  *mean = *std = 0.0;
  double s = 0;
  int i;
  RedisModuleString *cursor = C2S(ctx, "0");
  do {
    RedisModuleCallReply *zsrep = RedisModule_Call(ctx, "zscan", "sscl", key, cursor, "count", 100);
    RedisModuleCallReply *c = RedisModule_CallReplyArrayElement(zsrep, 0);
    RedisModuleCallReply *rep = RedisModule_CallReplyArrayElement(zsrep, 1);
    cursor = RedisModule_CreateStringFromCallReply(c);

    // RedisModule_Log(ctx, "warning", "cursor %s", RedisModule_StringPtrLen(cursor, NULL));
    // long long length = RedisModule_CallReplyLength(rep);
    // RedisModule_Log(ctx, "warning", "length %lld", length);
    for (i = 0; i < RedisModule_CallReplyLength(rep); i += 2) {
      RedisModuleCallReply *v = RedisModule_CallReplyArrayElement(rep, i+1);
      long long score = (long long)atoi(RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(v), NULL));
      // RedisModule_Log(ctx, "warning", "score %lld", score);
      *count += 1;
      *sum += score;
      s = s + 1.0 * (*count - 1) / *count * (score - *mean) * (score - *mean);
      *mean = *mean + 1.0 * (score - *mean) / *count;
      if (i == 0) {
        *min = *max = score;
      } else {
        if (score < *min) {
          *min = score;
        }
        if (score > *max) {
          *max = score;
        }
      }
    }
  } while(!RMUtil_StringEquals(cursor, C2S(ctx, "0")));
  if (*count > 1) {
    *std = sqrt(s / (*count - 1));
  }
  return;
}

void TimerHandler(RedisModuleCtx *ctx, void *data) {
  // RedisModule_Log(ctx, "warning", "aggregate timeout %lds + %ldns", interval_timer->interval.tv_sec, interval_timer->interval.tv_nsec);
  RedisModule_ThreadSafeContextLock(ctx);
  RedisModuleCallReply *vrep = RedisModule_Call(ctx, "sort", "ccccccc", "ab:versions", "by", "*", "get", "#", "get", "*->test");
  // RedisModule_Log(ctx, "warning", "versions %ld", RedisModule_CallReplyLength(vrep));
  RedisModuleCallReply *trep = RedisModule_Call(ctx, "sort", "ccccccccc", "ab:targets", "by", "*", "get", "#", "get", "*->test", "get", "*->value");
  // RedisModule_Log(ctx, "warning", "targets %ld", RedisModule_CallReplyLength(trep));
  int i, j;
  long long uv, pv, min, max;
  double mean, std;
  for (i = 0; i < RedisModule_CallReplyLength(vrep); i += 2) {
    RedisModuleCallReply *v = RedisModule_CallReplyArrayElement(vrep, i+1);
    RedisModuleString *vtest = RedisModule_CreateStringFromCallReply(v);
    // RedisModule_Log(ctx, "warning", "version test name %s", RedisModule_StringPtrLen(vtest, NULL));
    long long target_count = 0;
    RedisModuleString *version = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(vrep, i));
    for (j = 0; j < RedisModule_CallReplyLength(trep); j += 3) {
      RedisModuleCallReply *t = RedisModule_CallReplyArrayElement(trep, j+1);
      RedisModuleString *ttest = RedisModule_CreateStringFromCallReply(t);
      // RedisModule_Log(ctx, "warning", "target test name %s", RedisModule_StringPtrLen(ttest, NULL));
      RedisModuleString *target = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(trep, j+2));
      const char *ctarget = RedisModule_StringPtrLen(target, NULL);
      if (RMUtil_StringEquals(vtest, ttest)) {
        // RedisModule_Log(ctx, "warning", "version test %s eq target test %s", RedisModule_StringPtrLen(vtest, NULL), RedisModule_StringPtrLen(ttest, NULL));
        // track_key = ab:version:<test>:<value>:<target>:track
        RedisModuleString *track_key = RedisModule_CreateStringPrintf(
          ctx, "%s:%s:track",
          RedisModule_StringPtrLen(version, NULL),
          RedisModule_StringPtrLen(target, NULL)
        );
        // RedisModule_Log(ctx, "warning", "aggregate track %s", RedisModule_StringPtrLen(track_key, NULL));
        aggregate(ctx, track_key, &uv, &pv, &min, &max, &mean, &std);
        // RedisModule_Log(ctx, "warning", "aggregate track %lld %lld %lld %lld %f %f", uv, pv, min, max, mean, std);
        RedisModule_Call(
          ctx, "HSET", "sslslslslssss",
          version,
          RedisModule_CreateStringPrintf(ctx, "%s:user", ctarget), uv,
          RedisModule_CreateStringPrintf(ctx, "%s:count", ctarget), pv,
          RedisModule_CreateStringPrintf(ctx, "%s:min", ctarget), min,
          RedisModule_CreateStringPrintf(ctx, "%s:max", ctarget), max,
          RedisModule_CreateStringPrintf(ctx, "%s:mean", ctarget), RedisModule_CreateStringFromDouble(ctx, mean),
          RedisModule_CreateStringPrintf(ctx, "%s:std", ctarget), RedisModule_CreateStringFromDouble(ctx, std)
        );
        // RedisModule_Log(ctx, "warning", "aggregate track save version %s %lld %lld %lld %lld %f %f", RedisModule_StringPtrLen(version, NULL), uv, pv, min, max, mean, std);

        target_count += 1;
      }
    }
    if (target_count > 0) {
      RedisModuleString *uv_key = RedisModule_CreateStringPrintf(
        ctx, "%s:uv",
        RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(vrep, i)), NULL)
      );
      // RedisModule_Log(ctx, "warning", "aggregate uv %s", RedisModule_StringPtrLen(uv_key, NULL));
      aggregate(ctx, uv_key, &uv, &pv, &min, &max, &mean, &std);
      RedisModule_Call(
        ctx, "HSET", "sclclclclcscs",
        version,
        "pv", pv,
        "uv", uv,
        "uv:min", min,
        "uv:max", max,
        "uv:mean", RedisModule_CreateStringFromDouble(ctx, mean),
        "uv:std", RedisModule_CreateStringFromDouble(ctx, std)
      );
      // RedisModule_Log(ctx, "warning", "aggregate uv %lld %lld %lld %lld %f %f", uv, pv, min, max, mean, std);
    }
  }
  RedisModule_ThreadSafeContextUnlock(ctx);
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

  // register ab.track - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.track", TrackCommand);

  // register ab.traffic - using the shortened utility registration macro
  RMUtil_RegisterReadCmd(ctx, "ab.traffic", TrafficCommand);

  // register ab.rate - using the shortened utility registration macro
  RMUtil_RegisterReadCmd(ctx, "ab.rate", RateCommand);

  // register ab.timer - using the shortened utility registration macro
  // RMUtil_RegisterReadCmd(ctx, "ab.timer", TimerCommand);
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.timer", TimerCommand);
  // created timer
  interval_timer = RMUtil_NewPeriodicTimer(TimerHandler, NULL, NULL, (struct timespec){.tv_sec = 10, .tv_nsec = 0});
  // RedisModule_CreateTimer(ctx, timeout, TimerHandler, NULL);

  return REDISMODULE_OK;
}
