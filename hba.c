/*-------------------------------------------------------------------------
 * CS648: hba.c
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
#include <hbase/hbase.h>
#include "/home/sdn/project/project448/libhbase/src/test/native/common/byte_buffer.h"




#define HBASE_DATA_DIR "/tmp/"

typedef char HBasePath[101];


typedef struct cell_data_t_ {
  bytebuffer value;
  hb_cell_t  *hb_cell;
  struct cell_data_t_ *next_cell;
} cell_data_t;

typedef struct row_data_t_ {
  bytebuffer key;
  struct cell_data_t_ *first_cell;
} row_data_t;


static void
release_row_data(row_data_t *row_data) {
  if (row_data != NULL) {
    cell_data_t *cell = row_data->first_cell;
    while (cell) {
      bytebuffer_free(cell->value);
      free(cell->hb_cell);
      cell_data_t *cur_cell = cell;
      cell = cell->next_cell;
      free(cur_cell);
    }
    bytebuffer_free(row_data->key);
    free(row_data);
  }
}





static void
getHBasePath (HBasePath outBuffer, SMgrRelation reln, ForkNumber forknum)
{
	char *relp = relpath(reln->smgr_rnode, forknum);
	snprintf(outBuffer, sizeof(HBasePath), HBASE_DATA_DIR"%s", relp);
}


void hbainit (void)
{
	/* nothing to be done */
}





void
hbaclose (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}

/
bool hbaexists(SMgrRelation reln, ForkNumber forknum)
{
    int code;
	hb_connection_t connection = NULL;
    hb_client_t client = NULL;
    HBasePath hpath;
	getHBasePath(hpath,reln,forknum);
    code =hb_connection_create("localhost",hpath,&connection); 
    if(code != 0){
    	return true;
    }
    
	return false;
}


void
hbaprefetch (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
}



void hbaunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo)
{

}

void
hbatruncate (SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	/* nothing to be done */
}

void
hbaimmedsync (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}


void
hbaextend (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{
	hbawrite(reln, forknum, blocknum, buffer, skipFsync);
}


void
hbacreate (SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
    int code;
	hb_connection_t connection = NULL;
    hb_client_t client = NULL;
    HBasePath hpath;
	getHBasePath(hpath,reln,forkNum);
    code =hb_connection_create("localhost",hpath,&connection); 
    if(code == 0){
    	elog(DEBUG1,"connection created");
    }
    code = hb_client_create(connection, &client);
    if(code == 0){
    	elog(DEBUG1,"client created");
    }

    hb_connection_destroy(connection);


}

void
hbaread (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer)
{
   
	HBasePath hpath;
	hb_get_t get = NULL;
	hb_connection_t connection = NULL;
	hb_client_t client = NULL;
	getHBasePath(hpath,reln,forknum);
	hb_connection_create("localhost",hpath,&connection);
    hb_client_create(connection, &client);
    row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
	row_data->key->buffer = (char *)&blocknum;
	row_data->key->length = sizeof(blocknum);
	hb_get_create(row_data->key->buffer, row_data->key->length, &get);
	memcpy(buffer,&get,sizeof(&get));
    release_row_data(row_data);
    hb_connection_destroy(connection);



}


void
hbawrite (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{
        HBasePath hpath;
	    getHBasePath(hpath,reln,forknum);
	    hb_connection_t connection = NULL;
	    hb_client_t client = NULL;
        hb_put_t put = NU

        hb_connection_create("localhost",hpath,&connection);
        hb_client_create(connection, &client);
		
        
        row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
	    row_data->key->buffer = (char *)&blocknum;
	    row_data->key->length = sizeof(blocknum);
	    memcpy(row_data->first_cell->value,buffer,sizeof(buffer));
		hb_put_create(row_data->key->buffer, row_data->key->length, &put);

        release_row_data(row_data);
        hb_connection_destroy(connection);

}


BlockNumber
hbanblocks (SMgrRelation reln, ForkNumber forknum)
{
     int n;
     hb_scanner_t *scanner;
     HBasePath hpath;
	 hb_connection_t connection = NULL;
	 hb_client_t client = NULL;

	 
	 getHBasePath(hpath,reln,forknum);
	 hb_connection_create("localhost",hpath,&connection);
     hb_client_create(connection, &client);

     hb_scanner_create(hb_client_t client,scanner);
     if(hb_scanner_next(scanner,NULL, NULL)){
     	n++;
     }
     return ((BlockNumber) n);

}





