
#ifndef REDIS_H
#define REDIS_H

#include "postgres.h"
#include "storage/smgr.h"

#define REDIS_KEY_MAX_LEN 256

bool redis_exist(char *key);
int redis_read(char *key, char* buffer, int len);
int redis_write(char *key, char* buffer, int len);
int redis_write_str(char *key, char* str_value);
int redis_del(char *key);
int redis_del_keys(char *key);
uint32 redis_key_num(char *key);
void redis_free(void);
void redis_connect(void);

char* GetBlockKey(Oid dbNode, Oid spcNode, Oid relNode,
                  int backendId, ForkNumber forkNumber, BlockNumber block);
void redis_copy(char *src_key, char *dest_key);
void redis_copy_keys(char *key_prefix, char *new_key_prefix);


#endif
/* REDIS_H */