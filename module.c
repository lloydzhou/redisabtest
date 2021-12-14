#include "./redismodule.h"
#include "./rmutil/util.h"
#include "./rmutil/strings.h"
#include "./rmutil/test_util.h"
#include "./murmur.h"

/*
 * AB.VERSION <testname> [VALUE <version>] [WEIGHT <weight>] [NAME <version_name>]
*/
int VersionCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RedisModule_Log(ctx, "warning", "argc %d", argc);

  if (argc < 1 ) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 1) {
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "ccccccccccccc",
        "ab:versions", "BY", "ab:version:*->updated",
        "GET", "#", "GET", "ab:version:*->name", "GET", "ab:version:*->test",
        "GET", "ab:version:*->weight", "GET", "ab:version:*->updated"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModule_AutoMemory(ctx);

  RedisModuleString *test_key = RedisModule_CreateStringPrintf(ctx, "ab:version:%s", RedisModule_StringPtrLen(argv[1], NULL));
  if (argc == 2) {
    // SORT ab:version:test1 BY ab:version:*->updated get # get ab:version:*->name get ab:version:*->weight get ab:version:*->value
    RedisModuleCallReply *arep =
      RedisModule_Call(
        ctx, "SORT", "scccccccccccc",
        test_key, "BY", "ab:version:*->updated",
        "GET", "#", "GET", "ab:version:*->name", "GET", "ab:version:*->test",
        "GET", "ab:version:*->weight", "GET", "ab:version:*->updated"
      );
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
  }

  RedisModuleString *value = NULL;
  RMUtil_ParseArgsAfter("VALUE", argv, argc, "s", &value);
  if (value == NULL) {
    RedisModule_Log(ctx, "warning", "No version found");
    return RedisModule_ReplyWithError(ctx, "NEED VALUE");
  }
  RedisModuleString *version = RedisModule_CreateStringPrintf(ctx, "ab:version:%s", RedisModule_StringPtrLen(value, NULL));
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

  RedisModuleCallReply *srep =
      RedisModule_Call(ctx, "ZADD", "ccls", "ab:versions", "NX", ts, value);
  RMUTIL_ASSERT_NOERROR(ctx, srep);

  RedisModuleCallReply *lzrep =
      RedisModule_Call(ctx, "ZADD", "scls", test_key, "NX", ts, value);
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

  RedisModule_Log(ctx, "warning", "argc %d", argc);

  if (argc < 1 ) {
    return RedisModule_WrongArity(ctx);
  }

  if (argc == 1) {
    RedisModuleCallReply *arep =
      RedisModule_Call(ctx, "ZRANGE", "ccccc", "ab:tests", "-inf", "inf", "BYSCORE", "WITHSCORES");
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
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
    // TODO 计算对应的流量，再查找对应的版本，最后输出
    RedisModule_Log(ctx, "warning", "user %s", RedisModule_StringPtrLen(user, NULL));
    RedisModuleCallReply *arep =
        RedisModule_Call(ctx, "HGETALL", "s", var);
    RMUTIL_ASSERT_NOERROR(ctx, arep);

    return RedisModule_ReplyWithCallReply(ctx, arep);
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

  RedisModule_Log(ctx, "warning", "argc %d", argc);
  // we must have at least 4 args
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

  // register ab.version - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.version", VersionCommand);

  // register ab.value - using the shortened utility registration macro
  RMUtil_RegisterWriteDenyOOMCmd(ctx, "ab.value", VersionCommand);

  return REDISMODULE_OK;
}
