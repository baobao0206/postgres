//
// Created by Jinbao Chen on 2018/1/10.
//

#include "hiredis.h"
#include "postgres.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "access/xlog.h"
#include "common/relpath.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"
#include "storage/redis.h"


char *redis_ip = "127.0.0.1";
int redis_port = 6379;

redisContext *redis_con;


bool redis_exist(char *key) {
    redisReply *reply;
    reply = redisCommand(redis_con, "EXISTS %s", key);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis get err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis exits err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis exits type mis mach.")));
        freeReplyObject(reply);
    }
    return reply->integer > 0 ? true : false;
}

int redis_read(char *key, char* buffer, int len) {
    redisReply *reply;
    reply = redisCommand(redis_con, "GET %s", key);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis get err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis get err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_STRING) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis get type mis mach.")));
        freeReplyObject(reply);
    }
    if (reply->type == REDIS_REPLY_NIL) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis get not found.")));
        freeReplyObject(reply);
    }
    if (reply->len > len) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis value len is bigger buffer len.")));
        freeReplyObject(reply);
    }
    memset(buffer, 0, len);
    memcpy(buffer, reply->str, reply->len);
    freeReplyObject(reply);
    return (int)reply->len;
}

int redis_write(char *key, char* buffer, int len) {
    redisReply *reply;
    reply = redisCommand(redis_con, "SET %s %b", key, buffer, len);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis set err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis set err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_STATUS) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis set type mis mach.")));
        freeReplyObject(reply);
    }
    if (strcmp("OK", reply->str) != 0) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("set failed, status: %s.", reply->str)));
        freeReplyObject(reply);
    }
    freeReplyObject(reply);
    return 0;
}

int redis_write_str(char *key, char* str_value) {
    redisReply *reply;
    reply = redisCommand(redis_con, "SET %s %s", key, str_value);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis set err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis set err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_STATUS) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis set type mis mach.")));
        freeReplyObject(reply);
    }
    if (strcmp("OK", reply->str) != 0) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("set failed, status: %s.", reply->str)));
        freeReplyObject(reply);
    }
    freeReplyObject(reply);
    return 0;
}


int redis_del(char *key) {
    redisReply *reply;
    reply = redisCommand(redis_con, "DEL %s", key);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis del err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis del err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis del type mis mach.")));
        freeReplyObject(reply);
    }
    freeReplyObject(reply);
    return 0;
}

int redis_del_keys(char *key) {
    redisReply *reply;
    reply = redisCommand(redis_con, "KEYS %s", key);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis list err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis list err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis list type mis mach.")));
        freeReplyObject(reply);
    }
    for (int i = 0; i < reply->elements; ++i) {
        redis_del(reply->element[i]->str);
    }
    freeReplyObject(reply);
    return 0;
}

uint32 redis_key_num(char *key) {
    redisReply *reply;
    uint32 key_num;
    reply = redisCommand(redis_con, "KEYS %s", key);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis list err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis list err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis list type mis mach.")));
        freeReplyObject(reply);
    }
    key_num = (uint32)reply->elements;
    freeReplyObject(reply);
    return key_num;
}

void redis_copy(char *src_key, char *dest_key) {
    char buf[BLCKSZ];
    int len;
    len = redis_read(src_key, buf, BLCKSZ);
    redis_write(dest_key, buf, len);
}

void key_replace_prefix(char *dest, char *src, uint32 old_prefix_len, char* new_prefix) {
    strcpy(dest, new_prefix);
    if (old_prefix_len > strlen(src)) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("old prefix is longer than src, src: %s old_prefix_len %u new_prefix %s.",
                           src, old_prefix_len, new_prefix)));
    }
    strcpy(dest + strlen(new_prefix), src + old_prefix_len);
}

void redis_copy_keys(char *key_prefix, char *new_key_prefix) {
    char key_base[REDIS_KEY_MAX_LEN];
    char new_key[REDIS_KEY_MAX_LEN];
    redisReply *reply;
    sprintf(key_base, "%s%s", key_prefix, "*");
    reply = redisCommand(redis_con, "KEYS %s", key_base);
    if (reply == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("redis list err.")));
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis list err, str: %s.", reply->str)));
        freeReplyObject(reply);
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis list type mis mach.")));
        freeReplyObject(reply);
    }
    for (int i = 0; i < reply->elements; ++i) {
        memset(new_key, 0, REDIS_KEY_MAX_LEN);
        key_replace_prefix(new_key, reply->element[i]->str,
                           strlen(key_prefix), new_key_prefix);
        redis_copy(reply->element[i]->str, new_key);
    }
}

void redis_free() {
    if (redis_con) {
        redisFree(redis_con);
    }
}

void redis_connect() {
    if (redis_con != NULL) {
        redis_free();
    }
    redis_con = redisConnect(redis_ip, redis_port);
    if (redis_con == NULL) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("redis connect err.")));
    }
    if (redis_con->err != REDIS_OK) {
        ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("redis connect err, code: %d, str: %s.",
                           redis_con->err, redis_con->errstr)));
    }
}

