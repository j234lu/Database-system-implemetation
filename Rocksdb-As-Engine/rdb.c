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
#include "rocksdb/c.h"

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
void
rdbinit (void)
{
	/* nothing to be done */
}

/* CS448:
 *	rdbexists() -- is the relation/fork in RocksDB ?
 *
 * 	IMPORTANT: this function used to detect rdb tables.
 */
bool
rdbexists (SMgrRelation reln, ForkNumber forkNum)
{
	rocksdb_t *db = NULL;
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
	return false;
}

/* CS448:
 *	rdbclose() -- Close the specified relation, if it isn't closed already.
 *
 *	We open/close and flush to disk per write, so nothing to do here.
 */
void
rdbclose (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}

/* CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support pre-fetching from disk.
 *	See mdprefetch() in md.c for further details on what this was supposed to
 *	do.
 */
void
rdbprefetch (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
}

/*
 * CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support deletion so do nothing here.
 *	See mdunlink() in md.c for details on what this was supposed to do.
 */
void
rdbunlink (RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	/* do nothing */
}

/*
 * CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support deletion so do nothing here.
 *	See mdtruncate() in md.c for details on what this was supposed to do.
 */
void
rdbtruncate (SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
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
rdbimmedsync (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}


/* CS448:
 *	rdbextend() -- For our purpose this has the same semantics as rdbwrite().
 *
 *	See the comment above mdextend() in md.c for details.
 */
void
rdbextend (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{
	rdbwrite(reln, forknum, blocknum, buffer, skipFsync);
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
rdbcreate (SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{

	/*
	 * YOUR CODE HERE
	 * Hint: open a rocksdb connection from the reln
	 * create the rocksdb relation if not there using one of the rocksdb open options
	 * close the connection and free rocksdb resources  before exit
	 */


        /*
         * Do nothing if DB created and opend already
         */

        if (isRedo && rdbexists(reln,forkNum))
		return;


        /*
         * Create a DB if not already exists 
         */
        rocksdb_t *db;
	rocksdb_options_t *options = rocksdb_options_create();
	char *err = NULL;
        rocksdb_options_set_create_if_missing(options, 1);
        elog(DEBUG1, "New DB created");

        /*
         * Open DB
         */
	RocksDBPath path;
	getRocksDbPath(path, reln, forkNum);
        elog(DEBUG1, "Path set");
	db = rocksdb_open(options, path, &err);
        elog(DEBUG1, "DB opened");
        rocksdb_options_destroy(options);
        

        /*
         * Close if no exceptions thrown
         */
         if (!err)
	{
		rocksdb_close(db);
                elog(DEBUG1, "DB closed");
	}
}

/* CS448:
 *	rdbread() --  Read the specified block from a relation.
 */
void
rdbread (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer)
{

	/*
	 * YOUR CODE HERE
	 * Hint: open the rocksdb connection from reln before reading the block
	 * close the connection and free rocksdb resources  before exit
	 */

        /*
         * Open DB
         */
        rocksdb_t *db;
        rocksdb_options_t *options = rocksdb_options_create();
        
        char *err1 = NULL;
        char *err2 = NULL;
        RocksDBPath path;
        getRocksDbPath(path, reln, forknum);
        db = rocksdb_open(options, path, &err1);
        elog(DEBUG1, "Reading: DB opened. Error: %s.",err1);
        
        
        /*
         * Read from Block blocknum
         */
        elog(DEBUG1, "BlockNo: %u",blocknum);
        rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
        size_t len;
        char *returned_value = rocksdb_get(db, readoptions, (char*) &blocknum, sizeof(blocknum), &len, &err2);
        elog(DEBUG1, "Reading: Value Read: %s Error: %s.",returned_value,err2);
      //  buffer = rocksdb_get(db, readoptions, (char*) &blocknum, sizeof(blocknum), &len, &err2);
        memcpy(buffer,returned_value,len);
        elog(DEBUG1, "Reading: Memory copied, Buffer: %s",buffer);
        free(returned_value);
        
        /*
         * Close DB if no exceptions thrown
         */
        rocksdb_readoptions_destroy(readoptions);
        rocksdb_options_destroy(options);
        if (!err1 && !err2)
        {       
                rocksdb_close(db);
                elog(DEBUG1, "Reading: DB closed");
        }
}

/* CS448:
 *	rdbwrite() --  Write the supplied block at the appropriate location.
 *
 *	Note that we ignore skipFsync since it's used for checkpointing which we
 *	are not going to care about. We synchronously flush to disk anyways.
 */
void
rdbwrite (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{

		/*
		 * YOUR CODE HERE
		 * Hint: open the rocksdb connection from reln before writing the block
		 * close the connection and free rocksdb resources  before exit
		 */



                 /*
                  * Open DB
                 */
                 rocksdb_t *db;
       		 rocksdb_options_t *options = rocksdb_options_create();
       
       		 char *err1 = NULL;
       		 char *err2 = NULL;
                 RocksDBPath path;
        	 getRocksDbPath(path, reln, forknum);
       		 db = rocksdb_open(options, path, &err1);
       		 elog(DEBUG1, "Writing: DB opened. Error: %s.",err1);

                 /*
                  * Write to block blocknum
                  */
                 rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
                 
                 elog(DEBUG1, "BlockNo: %u",blocknum);
                 rocksdb_put(db, writeoptions, (char*) &blocknum, sizeof(blocknum), buffer, BLCKSZ, &err2);
                 elog(DEBUG1, "Writing: DB wirtten. Error: %s.",err2); 


                 /*
                  * Close DB if no exceptions thrown
                  */                
                 rocksdb_writeoptions_destroy(writeoptions);
                 rocksdb_options_destroy(options);
       		 if (!err1 && !err2)
       		 {
               		 rocksdb_close(db);
               		 elog(DEBUG1, "Write done: DB closed");
       		 }


}

/*
 * CS448:
 *	rdbnblocks() -- Calculate the number of blocks in the
 *					supplied relation.
 */
BlockNumber
rdbnblocks (SMgrRelation reln, ForkNumber forknum)
{

		/*
		 * YOUR CODE HERE
		 * Hint: open the rocksdb connection from reln before reading the block
		 * close the connection and free rocksdb resources  before exit
		 * You will need an iterator over rocksdb relation to count the number of blocks.
		 * Look for: rocksdb_create_iterator
		 *
		 */

                 /*
                  * Open DB and create an iterator
                  */
                 rocksdb_t *db;
                 rocksdb_options_t *options = rocksdb_options_create();
                 char *err = NULL;
                 RocksDBPath path;
                 getRocksDbPath(path, reln, forknum);
       		 int n=0;
                rocksdb_readoptions_t *readoptions = NULL;
	        rocksdb_iterator_t* it = NULL;
	       	readoptions = rocksdb_readoptions_create();
		db = rocksdb_open(options, path, &err);
		it = rocksdb_create_iterator(db, readoptions);

                /*
                 * Iterate through the file and count keys
                 */
                for(rocksdb_iter_seek_to_first(it);rocksdb_iter_valid(it);rocksdb_iter_next(it)){
                         n++;   
                }


               /*
                * Close DB if no exceptions thrown
                */
               rocksdb_readoptions_destroy(readoptions);
               rocksdb_options_destroy(options); 
               rocksdb_iter_destroy(it);
               if (!err)
                 {
                         rocksdb_close(db);
                         elog(DEBUG1, "BlockNo: DB closed. N: %d.",n);
                 }
              /*
               * Return the block number
               */
              return ((BlockNumber) n);
}


