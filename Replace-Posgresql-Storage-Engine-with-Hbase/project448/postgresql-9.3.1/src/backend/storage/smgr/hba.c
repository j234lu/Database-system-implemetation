/*-------------------------------------------------------------------------
 * CS448:
 * rdb.c
 *	  This code manages relations stored in RocksDB.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/rdb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "catalog/catalog.h"
#include "common/relpath.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "hbase/hbase.h"


/* CS448: helper functions */

/* Location to store RocksDB files. */
#define ROCKSDB_DATA_DIR "/tmp/"

typedef char RocksDBPath[101];


static char*
replaceChar (char *relp)
{
	/* Replace / with _ . */
	int i = 0;
	while (relp[i] != '\0')
	{
		if (relp[i] == '/')
		{
			relp[i] = '_';
		}
		i++;
	}
	return relp;
}

/*
 * Gets a relation name from SMgrRelation and returns it in RocksDBPath
 */

static void
getRocksDbPath (RocksDBPath outBuffer, SMgrRelation reln, ForkNumber forknum)
{
	char *relp = relpath(reln->smgr_rnode, forknum);
	relp = replaceChar(relp);
	snprintf(outBuffer, sizeof(RocksDBPath), ROCKSDB_DATA_DIR"%s", relp);
}

/* end CS448: helper functions */

/* CS448:
 *	rdbinit() -- called during backend startup to initialize.
 */
void hbainit (void)
{
	/* nothing to be done */
}





/* CS448:
 *	rdbclose() -- Close the specified relation, if it isn't closed already.
 *
 *	We open/close and flush to disk per write, so nothing to do here.
 */
void
hbaclose (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}

/* CS448:
 *	rdbexists() -- is the relation/fork in RocksDB ?
 *
 * 	IMPORTANT: this function used to detect rdb tables.
 */
bool hbaexists(SMgrRelation reln, ForkNumber forknum)
{
 /*	rocksdb_t *db = NULL;
	rocksdb_options_t *options = NULL;
	char *err = NULL;
	RocksDBPath path;

	options = rocksdb_options_create();
	getRocksDbPath(path, reln, forkNum);
	db = rocksdb_open(options, path, &err);
	rocksdb_options_destroy(options);
	if (!err)
	{
		rocksdb_close(db);
		return true;
	}
	return false;*/
	return false;
}

/* CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support pre-fetching from disk.
 *	See mdprefetch() in md.c for further details on what this was supposed to
 *	do.
 */
void
hbaprefetch (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
}

/*
 * CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support deletion so do nothing here.
 *	See mdunlink() in md.c for details on what this was supposed to do.
 */

void hbaunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo)
{

}
/*
 * CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support deletion so do nothing here.
 *	See mdtruncate() in md.c for details on what this was supposed to do.
 */
void
hbatruncate (SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	/* nothing to be done */
}

/*
 * CS448:
 *	rdbimmedsync() -- Force the specified relation to stable storage.
 *
 *	We flush to disk per write, so nothing to do here.
 */
void
hbaimmedsync (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}


/* CS448:
 *	rdbextend() -- For our purpose this has the same semantics as rdbwrite().
 *
 *	See the comment above mdextend() in md.c for details.
 */
void
hbaextend (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{
	hbawrite(reln, forknum, blocknum, buffer, skipFsync);
}


/*
 *  ============================================================================================================
 *  DO NOT CHANGE FUNCTIONS ABOVE THIS LINE
 *  ============================================================================================================
 */


/* CS448:
 *	rdbcreate() -- Creates a relation in RocksDB.
 */
void
hbacreate (SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{

	/*
	 * YOUR CODE HERE
	 * Hint: open a rocksdb connection from the reln
	 * create the rocksdb relation if not there using one of the rocksdb open options
	 * close the connection and free rocksdb resources  before exit
	 */
}

/* CS448:
 *	rdbread() --  Read the specified block from a relation.
 */
void
hbaread (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer)
{

	/*
	 * YOUR CODE HERE
	 * Hint: open the rocksdb connection from reln before reading the block
	 * close the connection and free rocksdb resources  before exit
	 */
}

/* CS448:
 *	rdbwrite() --  Write the supplied block at the appropriate location.
 *
 *	Note that we ignore skipFsync since it's used for checkpointing which we
 *	are not going to care about. We synchronously flush to disk anyways.
 */
void
hbawrite (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{

		/*
		 * YOUR CODE HERE
		 * Hint: open the rocksdb connection from reln before writing the block
		 * close the connection and free rocksdb resources  before exit
		 */

}

/*
 * CS448:
 *	rdbnblocks() -- Calculate the number of blocks in the
 *					supplied relation.
 */
BlockNumber
hbanblocks (SMgrRelation reln, ForkNumber forknum)
{

		/*
		 * YOUR CODE HERE
		 * Hint: open the rocksdb connection from reln before reading the block
		 * close the connection and free rocksdb resources  before exit
		 * You will need an iterator over rocksdb relation to count the number of blocks.
		 * Look for: rocksdb_create_iterator
		 *
		 */

}





