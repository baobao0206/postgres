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

char* GetBlockKey(Oid dbNode, Oid spcNode, Oid relNode,
                  int backendId, ForkNumber forkNumber, BlockNumber block) {
    char* path;
    path = GetRelationPath(dbNode, spcNode, relNode, backendId, forkNumber);
    path = psprintf("%s/%u", path, block);
    return path;
}

/* First argument is a RelFileNode */
#define blockkeybackend(rnode, backend, forknum, block) \
	GetBlockKey((rnode).dbNode, (rnode).spcNode, (rnode).relNode, \
                backend, forknum, block)

/* First argument is a RelFileNode */
#define blockkeyperm(rnode, forknum, block) \
	blockkeybackend(rnode, InvalidBackendId, forknum, block)

/* First argument is a RelFileNodeBackend */
#define blockkey(rnode, forknum, block) \
	blockkeybackend((rnode).node, (rnode).backend, forknum, block)

void rdinit() {
    redis_connect();
}

void
rdclose(SMgrRelation reln, ForkNumber forknum) {
}

void
rdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo) {
    char* rel_key;
    rel_key = relpath(reln->smgr_rnode, forkNum);
    if (rdexists(reln, forkNum)) {
        if (isRedo) {
            return;
        } else {
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("create rel '%s' but it has exist.", rel_key)));
        }
    }
    redis_write_str(rel_key, "ok");
    pfree(rel_key);
}

bool
rdexists(SMgrRelation reln, ForkNumber forkNum) {
    char* rel_key;
    bool exist;
    rel_key = relpath(reln->smgr_rnode, forkNum);
    exist = redis_exist(rel_key);
    pfree(rel_key);
    return exist;
}

void
rdunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo) {
    char *rel_key;
    char rel_block_key_base[REDIS_KEY_MAX_LEN];
    rel_key = relpath(rnode, forkNum);
    if (forkNum != InvalidForkNumber) {
        return;
    }
    if (redis_exist(rel_key) == false) {
        if (isRedo) {
            return;
        } else {
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("del rel '%s' but it not exist.", rel_key)));
        }
    }
    redis_del(rel_key);
    sprintf(rel_block_key_base, "%s/%s", rel_key, "*");
    redis_del_keys(rel_block_key_base);
    pfree(rel_key);
}

void
rdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
         char *buffer, bool skipFsync) {
    char* rel_key;
    rel_key = blockkey(reln->smgr_rnode, forknum, blocknum);
    redis_write(rel_key, buffer, BLCKSZ);
    pfree(rel_key);
}

void
rdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum) {

}

void
rdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
       char *buffer) {
    char *rel_key;
    rel_key = blockkey(reln->smgr_rnode, forknum, blocknum);
    redis_read(rel_key, buffer, BLCKSZ);
    pfree(rel_key);
}

void
rdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
        char *buffer, bool skipFsync) {
    char *rel_key;
    rel_key = blockkey(reln->smgr_rnode, forknum, blocknum);
    redis_write(rel_key, buffer, BLCKSZ);
    pfree(rel_key);
}

void
rdwriteback(SMgrRelation reln, ForkNumber forknum,
            BlockNumber blocknum, BlockNumber nblocks) {

}

BlockNumber
rdnblocks(SMgrRelation reln, ForkNumber forknum) {
    char *rel_key;
    uint32 block_number;
    rel_key = relpath(reln->smgr_rnode, forknum);
    sprintf(rel_key, "%s/%s", rel_key, "*");
    block_number = redis_key_num(rel_key);
    pfree(rel_key);
    return block_number;
}

void
rdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks) {
    char *rel_key;
    uint32 curnblk = rdnblocks(reln, forknum);
    if (curnblk < nblocks) {
        if (InRecovery)
            return;
        ereport(ERROR,
                (errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
                        relpath(reln->smgr_rnode, forknum),
                        nblocks, curnblk)));
    }
    if (curnblk == nblocks) {
        return;
    }
    for (int i = nblocks; i < curnblk; ++i) {
        rel_key = blockkey(reln->smgr_rnode, forknum, i);
        redis_del(rel_key);
    }
    pfree(rel_key);
}

void
rdimmedsync(SMgrRelation reln, ForkNumber forknum) {

}

void
rdpreckpt(void) {

}

void
rdsync(void) {

}

void
rdpostckpt(void) {

}



