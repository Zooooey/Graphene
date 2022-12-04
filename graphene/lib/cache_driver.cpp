#include "cache_driver.h"
#include <sys/mman.h>
//#include <asm/mman.h>
#include <linux/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <stdint.h>
using namespace std;
//bitmap + req_list together
cache_driver::cache_driver(
			int fd_csr,
			bit_t* &reqt_blk_bitmap,
			index_t* &reqt_list,
			index_t *reqt_blk_count,
			const index_t total_blks,
			vertex_t *blk_beg_vert,
			bool *io_conserve,
			const index_t num_chunks,
			const size_t chunk_sz,
			const index_t io_limit,
			index_t MAX_USELESS):
	cache_driver(fd_csr, reqt_blk_bitmap, reqt_blk_count,
			total_blks, blk_beg_vert, io_conserve, 
			num_chunks, chunk_sz, io_limit, MAX_USELESS)
{
	this->reqt_list = reqt_list;
}


//bitmap only implementation
cache_driver::cache_driver(
			int fd_csr,
			bit_t* &reqt_blk_bitmap,
			index_t *reqt_blk_count,
			const index_t total_blks,
			vertex_t *blk_beg_vert,
			bool *io_conserve,
			const index_t num_chunks,
			const size_t chunk_sz,
			const index_t io_limit,
			index_t MAX_USELESS):
	num_chunks(num_chunks),fd_csr(fd_csr),
	reqt_blk_count(reqt_blk_count),
	chunk_sz(chunk_sz),io_conserve(io_conserve),
	reqt_blk_bitmap(reqt_blk_bitmap),
	total_blks(total_blks),io_limit(io_limit),
	blk_beg_vert(blk_beg_vert),MAX_USELESS(MAX_USELESS)
{
	fetch_sz=0;

	io_submit_time = 0;
	io_poll_time = 0;
	VERT_PER_BLK=BLK_SZ/sizeof(vertex_t);
	load_blk_off = 0;
	coarse_grain_off = 0;
	vert_per_chunk = chunk_sz / sizeof(vertex_t);
	blk_per_chunk = vert_per_chunk/VERT_PER_BLK;

	//time out spec
	time_out = new struct timespec;
	time_out->tv_sec = 0;
	time_out->tv_nsec = 30000;
		
	//using a big buffer for all chunks
	buff=NULL;
	buff=(vertex_t *)mmap(NULL,chunk_sz*num_chunks,
		PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
		| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(buff==MAP_FAILED)
	{
		perror("buffer mmap");
		exit(-1);
	}
	
	//alloc buff space
	cache = new struct chunk*[num_chunks];
	for(index_t i=0;i<num_chunks;i++)
	{
		cache[i] = new struct chunk;
		cache[i]->load_sz = -1;
		cache[i]->blk_beg_off = -1;
		//try alignment of big array here
		cache[i]->status = EVICTED;
		cache[i]->buff = &(buff[i*vert_per_chunk]);
	}
	
	//alloc io list
	io_list = new struct io_req*[io_limit];
	piocb = new struct iocb*[MAX_EVENTS];
	for(index_t i=0;i<io_limit; ++i)
	{
		io_list[i] = new struct io_req;
		io_list[i]->io_cbp = new struct iocb[MAX_EVENTS];
		
		io_list[i]->num_ios=0;
		io_list[i]->ctx=0;
		io_list[i]->chunk_id = new index_t[MAX_EVENTS];
		if(io_setup(MAX_EVENTS,&(io_list[i]->ctx))!=0)
		{
			perror("io_setup error\n");
			exit(-2);
		}
	}

	events = new struct io_event[MAX_EVENTS];
	circ_free_chunk = new circle(num_chunks);
	circ_load_chunk = new circle(num_chunks);
	circ_free_ctx = new circle(io_limit);
	circ_submitted_ctx = new circle(io_limit);
	
	//every chunk should be en_circled into free chunk
	for(index_t i = 0; i < num_chunks; i++)
		circ_free_chunk->en_circle(i);
	
	for(index_t i = 0; i < io_limit; i++)
		circ_free_ctx->en_circle(i);
}

cache_driver::~cache_driver()
{
	munmap(buff,chunk_sz*num_chunks);
	for(index_t i=0;i<io_limit;i++)
	{
		if(io_destroy(io_list[i]->ctx)<0)
			perror("io_destroy");
		delete io_list[i]->io_cbp;
		delete io_list[i];
	}
	close(fd_csr);
	delete[] piocb;

	delete[] io_list;
	delete events; 
	delete circ_free_chunk;
	delete circ_load_chunk;
	delete circ_free_ctx;
	delete circ_submitted_ctx;
}

void cache_driver::submit_io_req(index_t io_id)
{
	int ret;
	assert(io_list[io_id]->num_ios<=MAX_EVENTS);
	for(index_t i=0;i<io_list[io_id]->num_ios;i++)
	{
		//Record the address for submission
		piocb[i]=&(io_list[io_id]->io_cbp[i]);
		index_t chunk_id = io_list[io_id]->chunk_id[i];
		
		io_prep_pread(&(io_list[io_id]->io_cbp[i]), 
				fd_csr,
				cache[chunk_id]->buff, 
				cache[chunk_id]->load_sz*sizeof(vertex_t),
				cache[chunk_id]->blk_beg_off*sizeof(vertex_t));

		//if(pread(fd_csr, cache[chunk_id]->buff, 
		//			cache[chunk_id]->load_sz*sizeof(vertex_t),	
		//			cache[chunk_id]->blk_beg_off*sizeof(vertex_t))<=0)
		//{
		//	perror("pread");
		//	std::cout<<cache[chunk_id]->load_sz<<" "
		//		<<cache[chunk_id]->blk_beg_off<<"\n";
		//	exit(-1);
		//}
		
		fetch_sz += cache[chunk_id]->load_sz * sizeof(vertex_t);
	}

//	if(omp_get_thread_num() == 1)
//		std::cout<<"Submitted "<<cache[chunk_id]->beg_vert<<" "
//		<<cache[chunk_id]->load_sz<<" "
//		<<cache[chunk_id]->blk_beg_off*sizeof(vertex_t)
//		<<"\n";
//	
	if((ret=io_submit(io_list[io_id]->ctx, 
				io_list[io_id]->num_ios, 
				piocb ))!=io_list[io_id]->num_ios)
	{
		perror("io_submit");
		printf("submitted: %d of %ld\n",ret,io_list[io_id]->num_ios);
		for(index_t i=0;i<io_list[io_id]->num_ios;i++)
		{
			index_t chunk_id = io_list[io_id]->chunk_id[i];
			std::cout<<"size-beg: "<<cache[chunk_id]->load_sz<<" "
				<<cache[chunk_id]->blk_beg_off<<"\n";
		}
		exit(-1);
	}
}


//for Bitmap based IO loading
//从free_chunk取出chunk并发动io载入数据
void cache_driver::load_chunk()
{
	//Let cache driver do some work.
	//-If there is work to do
	//TODO!!! THIS IS A BIG ISSUE!
	//load_blk_off should be inited by someone else.
	double  this_time = wtime();
	//io_conserve貌似是一种重置，类须于下一个level需要把io给重制了？
	if(*io_conserve)
	{
		cout<<"[cache_driver::load_chunk]: Doing io_converv! free_chunk&load_chunk would be reset!"<<endl;
		//printf("*reqt_blk_count: %ld\n", *reqt_blk_count);
		//- load_blk_off: next load will load from this block 
		load_blk_off = 0;
		*io_conserve = false;
		
		//reset circ_free_chunk
		circ_free_chunk->reset_circle();
		circ_load_chunk->reset_circle();
		for(int i = 0; i < num_chunks; i ++)
		{
			//index_t beg_blk_id=cache[i]->blk_beg_off/VERT_PER_BLK;
			//index_t end_blk_id=beg_blk_id+cache[i]->load_sz/VERT_PER_BLK;
			//bool is_reuse=false;
			//while(beg_blk_id<end_blk_id)
			//{
			//	if(reqt_blk_bitmap[beg_blk_id>>3] & (1<<(beg_blk_id&7)))
			//	{
			//		if(!is_reuse) is_reuse = true;
			//		--(*reqt_blk_count);
			//		reqt_blk_bitmap[beg_blk_id>>3] &= (~(1<<(beg_blk_id&7)));	
			//	}
			//	beg_blk_id++;
			//}

			//if(is_reuse)
			//{
			//	cache[i]->status=LOADED;
			//	circ_load_chunk->en_circle(i);
			//}
			//else
			//{
			cache[i]->status=EVICTED;
			circ_free_chunk->en_circle(i);
			//}
		}
	}
	
	if(circ_free_ctx->get_sz()==0){
		cout<<"[cache_driver::load_chunk]:No free ctx, aborting load_chunk()"<<endl;
		return;
	}
	
	//得到一个free_ctx
	index_t io_id = circ_free_ctx->de_circle();
	//从io_list里获得io_req对象
	struct io_req *req = io_list[io_id];
	req->num_ios = 0;
	
	if(load_blk_off >= total_blks && (*reqt_blk_count != 0))
	{
		std::cout<<"Exhaust all blocks not finish all requests\n";
		//assert(*reqt_blk_count == 0);
		*reqt_blk_count = 0;

	}

	while((load_blk_off<total_blks) && (*reqt_blk_count != 0))
	{
		//find a to-be-load block
		/*这个位操作貌似是用来确定这个block要不要读取的.
			右移三位表示load_blk_off的最低3位有别的用途，只需要高3位以后的数值作为index
			load_blk_off= 0000 0001 1101 1101(b)
						  | bit_map index|value|
			load_blk_off >>3 = index = 0000 0001 1101 1xxx(b)
			load_blk_off&7 = xxxx xxxx xxxx x101(b)
			说明load_blk_off的低3位范围是十进制的[0,7]
			1<<(load_blk_off&7) 是 0000 0000 8个位里置1，因此有8种可能。

			load_blk_off
		*/

		
		if(reqt_blk_bitmap[load_blk_off>>3] & (1<<(load_blk_off&7)))//8个位里对应的那一位是1
		{
			//Get one free chunk
			//-record this chunk is submmited
			if(circ_free_chunk->get_sz() == 0)
			{
				std::cout<<"[cache_driver::load_chunk]:Running out of free chunk! Aborting load_chunk()\n";
				break;
			}
			
			//从环形缓冲区获取一个free_chunk
			index_t chunk_id = circ_free_chunk->de_circle();
			//在req对象的里标记这个chunk的id
			req->chunk_id[req->num_ios++] = chunk_id;

			
			--(*reqt_blk_count);//TODO: 这里为什么要--?reqt_blk_count代表着什么？我猜应该是指还有多少blk需要读取，--就是目前读了一个？
			cout<<"reqt_blk_count:"<<*reqt_blk_count<<endl;
			//clean this bit
			/*
				这里按位取反其实是取消置位，例如0000 0001取反是1111 1110，然后与操作会把最低置0
			*/
			//取消置位的原因应该也是，reqt_blk_bitmap标记了哪些blk需要读取。
			reqt_blk_bitmap[load_blk_off>>3] &= (~(1<<(load_blk_off&7)));
			
			//update chunk metadata
			//更新metadata，并标记 位loading。
			cache[chunk_id]->status = LOADING;
			index_t beg_blk_id = load_blk_off;
			cache[chunk_id]->beg_vert= blk_beg_vert[beg_blk_id];
			cache[chunk_id]->blk_beg_off=VERT_PER_BLK * beg_blk_id;
			cache[chunk_id]->load_sz = VERT_PER_BLK;
			
			//Loading is done
			if(*reqt_blk_count == 0)
			{
				goto finishpoint;
			}

			//Unite blocks to form a big chunk
			int continuous_useless_blk=0;//底下blk要读连续的blk，所以这个变量标记空洞的数量。
			while(load_blk_off<total_blks)
			{
				++load_blk_off;
				//应该是说超过一个chunk？因为load_chunk本质上是只处理一个chunk吧？
				if(load_blk_off+1-beg_blk_id>blk_per_chunk) break;
				
				if(reqt_blk_bitmap[load_blk_off>>3] & (1<<(load_blk_off&7)))
				{
					//continuous_useless_blk=0;

					--(*reqt_blk_count);
					reqt_blk_bitmap[load_blk_off>>3] &= (~(1<<(load_blk_off&7)));
					//这里更新下当前chunk的装载数据的大小，会把空洞也算进去，所以底下continous_useless_blk的作用就是记录这里有点多少废数据？
					cache[chunk_id]->load_sz=(load_blk_off+1-beg_blk_id)*VERT_PER_BLK;
					
					//init load_blk_off for next level scanning
					if(*reqt_blk_count == 0)
					{
						goto finishpoint;
					}
				}
				else
				{
					continuous_useless_blk++;
					if(continuous_useless_blk==MAX_USELESS) break;
				}
			}
			
			//io数到头了
			if(req->num_ios==MAX_EVENTS)
			{
				//avoid io_ctx miss tracking
				circ_submitted_ctx->en_circle(io_id);//ctx已提交的环形缓冲区
				submit_io_req(io_id);


				//start a new io_ctx;
				//如果free_ctx好像是放出一个free_ctx给下一个loop用的。
				if(circ_free_ctx->get_sz()==0)
				{
					io_submit_time += (wtime() - this_time);
					return;
				}
				//这里放出来给下一个loop？
				io_id = circ_free_ctx->de_circle();
				req = io_list[io_id];
				req->num_ios=0;
			}
		}
		else	++load_blk_off;
	}
	
finishpoint:
	//avoid io_ctx miss tracking
	if(io_list[io_id]->num_ios != 0)
	{
		circ_submitted_ctx->en_circle(io_id);
		submit_io_req(io_id);
	}
	else circ_free_ctx->en_circle(io_id);
	io_submit_time += (wtime() - this_time);

	//if(load_blk_off>=total_blks)
	//{
	//	std::cout<<" I am done because I finished all blks???\n";
	//}
}


//Require the reqt-list is sorted
void cache_driver::load_chunk_iolist()
{
	//Let cache driver do some work.
	//-If there is work to do
	//TODO!!! THIS IS A BIG ISSUE!
	//load_blk_off should be inited by someone else.
	double  this_time = wtime();
	if(*io_conserve)
	{
		//printf("*reqt_blk_count: %ld\n", *reqt_blk_count);
		//- load_blk_off: next load will load from this block 
		load_blk_off = 0;
		*io_conserve = false;
		
		//reset circ_free_chunk
		circ_free_chunk->reset_circle();
		circ_load_chunk->reset_circle();
		for(int i = 0; i < num_chunks; i ++)
		{
			//index_t beg_blk_id=cache[i]->blk_beg_off/VERT_PER_BLK;
			//index_t end_blk_id=beg_blk_id+cache[i]->load_sz/VERT_PER_BLK;
			//bool is_reuse=false;
			//while(beg_blk_id<end_blk_id)
			//{
			//	if(reqt_blk_bitmap[beg_blk_id>>3] & (1<<(beg_blk_id&7)))
			//	{
			//		if(!is_reuse) is_reuse = true;
			//		--(*reqt_blk_count);
			//		reqt_blk_bitmap[beg_blk_id>>3] &= (~(1<<(beg_blk_id&7)));	
			//	}
			//	beg_blk_id++;
			//}

			//if(is_reuse)
			//{
			//	cache[i]->status=LOADED;
			//	circ_load_chunk->en_circle(i);
			//}
			//else
			//{
			cache[i]->status=EVICTED;
			circ_free_chunk->en_circle(i);
			//}
		}
	}
	
	if(circ_free_ctx->get_sz()==0)
		return;
	
	index_t io_id = circ_free_ctx->de_circle();
	struct io_req *req = io_list[io_id];
	req->num_ios = 0;

	while(load_blk_off < (*reqt_blk_count))
	{
		//Get one free chunk
		//-record this chunk is submmited
		if(circ_free_chunk->get_sz() == 0)
		{
			//std::cout<<"Running out of chunk$$$$$$$$\n";
			break;
		}

		index_t chunk_id = circ_free_chunk->de_circle();
		req->chunk_id[req->num_ios++] = chunk_id;

		//update chunk metadata
		//With the first to-be-loaded block
		cache[chunk_id]->status = LOADING;
		index_t beg_blk_id = reqt_list[load_blk_off];
		cache[chunk_id]->beg_vert = blk_beg_vert[beg_blk_id];
		cache[chunk_id]->blk_beg_off = VERT_PER_BLK * beg_blk_id;
		cache[chunk_id]->load_sz = VERT_PER_BLK;
		load_blk_off ++;
		
		//Unite blocks to form a big chunk
		while(load_blk_off < (*reqt_blk_count))
		{
			index_t blk_id = reqt_list[load_blk_off];
			if(blk_id < reqt_list[load_blk_off - 1]) break;
			if(blk_id + 1 - beg_blk_id > blk_per_chunk) break;
			if(blk_id - reqt_list[load_blk_off - 1] >= MAX_USELESS) break;
			cache[chunk_id]->load_sz = (blk_id+1-beg_blk_id)*VERT_PER_BLK;
		//	if(cache[chunk_id]->load_sz < 0) 
		//	{
		//		printf("blk_id %d, beg_blk_id %d, total %d\n", 
		//				blk_id, beg_blk_id, *reqt_blk_count);
		//	}

			load_blk_off ++;
		}
		
		//For IO iterator termination
		if(load_blk_off >= (*reqt_blk_count)) *reqt_blk_count = 0;

		if(req->num_ios == MAX_EVENTS)
		{
			//avoid io_ctx miss tracking
			circ_submitted_ctx->en_circle(io_id);
			submit_io_req(io_id);

			//start a new io_ctx;
			if(circ_free_ctx->get_sz()==0)
			{
				io_submit_time += (wtime() - this_time);
				return;
			}

			io_id = circ_free_ctx->de_circle();
			req = io_list[io_id];
			req->num_ios = 0;
		}
	}

	//avoid io_ctx miss tracking
	if(io_list[io_id]->num_ios != 0)
	{
		circ_submitted_ctx->en_circle(io_id);
		submit_io_req(io_id);
	}
	else circ_free_ctx->en_circle(io_id);
	io_submit_time += (wtime() - this_time);
		
}

//for Full IO
void cache_driver::load_chunk_full()
{
	//Let cache driver do some work.
	//-If there is work to do
	//TODO!!! THIS IS A BIG ISSUE!
	//load_blk_off should be inited by someone else.
	double  this_time = wtime();
	if(*io_conserve)
	{
		//printf("*reqt_blk_count: %ld\n", *reqt_blk_count);
		//- load_blk_off: next load will load from this block 
		load_blk_off = 0;
		*io_conserve = false;
		
		//reset circ_free_chunk
		circ_free_chunk->reset_circle();
		circ_load_chunk->reset_circle();
		for(int i = 0; i < num_chunks; i ++)
		{
			//index_t beg_blk_id=cache[i]->blk_beg_off/VERT_PER_BLK;
			//index_t end_blk_id=beg_blk_id+cache[i]->load_sz/VERT_PER_BLK;
			//bool is_reuse=false;
			//while(beg_blk_id<end_blk_id)
			//{
			//	if(reqt_blk_bitmap[beg_blk_id>>3] & (1<<(beg_blk_id&7)))
			//	{
			//		if(!is_reuse) is_reuse = true;
			//		--(*reqt_blk_count);
			//		reqt_blk_bitmap[beg_blk_id>>3] &= (~(1<<(beg_blk_id&7)));	
			//	}
			//	beg_blk_id++;
			//}

			//if(is_reuse)
			//{
			//	cache[i]->status=LOADED;
			//	circ_load_chunk->en_circle(i);
			//}
			//else
			//{
			cache[i]->status=EVICTED;
			circ_free_chunk->en_circle(i);
			//}
		}
	}
	
	if(circ_free_ctx->get_sz()==0)
		return;
	
	index_t io_id = circ_free_ctx->de_circle();
	struct io_req *req = io_list[io_id];
	req->num_ios = 0;

	while(load_blk_off < total_blks)
	{
		//Get one free chunk
		//-record this chunk is submmited
		if(circ_free_chunk->get_sz() == 0)
		{
			//std::cout<<"Running out of chunk$$$$$$$$\n";
			break;
		}

		index_t chunk_id = circ_free_chunk->de_circle();
		req->chunk_id[req->num_ios++] = chunk_id;

		//update chunk metadata
		//With the first to-be-loaded block
		cache[chunk_id]->status = LOADING;
		index_t beg_blk_id = load_blk_off;
		cache[chunk_id]->beg_vert = blk_beg_vert[beg_blk_id];
		cache[chunk_id]->blk_beg_off = VERT_PER_BLK * beg_blk_id;

		//Unite blocks to form a big chunk
		if((load_blk_off + blk_per_chunk) < total_blks)
		{
			load_blk_off += blk_per_chunk;
			cache[chunk_id]->load_sz = vert_per_chunk;
		}
		else
		{
			cache[chunk_id]->load_sz = (total_blks - beg_blk_id)*VERT_PER_BLK;
			load_blk_off = total_blks;
		}

		if(req->num_ios == MAX_EVENTS)
		{
			//avoid io_ctx miss tracking
			circ_submitted_ctx->en_circle(io_id);
			submit_io_req(io_id);

			//start a new io_ctx;
			if(circ_free_ctx->get_sz()==0)
			{
				io_submit_time += (wtime() - this_time);
				return;
			}


			io_id = circ_free_ctx->de_circle();
			req = io_list[io_id];
			req->num_ios = 0;
		}
	}
	
	//For IO iterator termination
	if(load_blk_off >= total_blks) *reqt_blk_count = 0;

	//avoid io_ctx miss tracking
	if(io_list[io_id]->num_ios != 0)
	{
		circ_submitted_ctx->en_circle(io_id);
		submit_io_req(io_id);
	}
	else circ_free_ctx->en_circle(io_id);
	io_submit_time += (wtime() - this_time);
		
}


//return a full circle of loaded chunks
//从submitted_ctx里取出所有已有数据的chunk，放入load_chunk circle，然后释放ctx到free_ctx里。
circle *cache_driver::get_chunk()
{
	//std::cout<<"here\n";
	//Once entering here. 
	//-dump out at least half submissions
	//-if at half, still NULL, dump all
	int ret, n;
	const int check_limit = 8;
	int check_count = 0;
	double  this_time = wtime();
	while(true)
	{

		index_t io_id = circ_submitted_ctx->de_circle();
		if(io_id == -1)
		{
			break;
		}
		
		struct io_req *req=io_list[io_id];
//		std::cout<<"waiting io: "<<req->num_ios<<"\n";
		ret=n=io_getevents(req->ctx,req->num_ios,MAX_EVENTS,
					events,NULL);
//		std::cout<<"return waiting\n";
		if(ret!=req->num_ios)	
		{
			perror("io_getevents error\n");
			std::cout<<req->num_ios<<" "<<MAX_EVENTS<<"\n";
			continue;
		}

		for(int i=0;i<req->num_ios;i++)
		{
			//std::cout<<"Get-a-chunk\n";
			index_t chunk_id = req->chunk_id[i];
			cache[chunk_id]->status = LOADED;
			circ_load_chunk->en_circle(chunk_id);
		}
		
		req->num_ios=0;	
		circ_free_ctx->en_circle(io_id);
		
		//almost full
		if(circ_load_chunk->get_sz()
			>=circ_load_chunk->size-MAX_EVENTS) 
				break;

		check_count ++;
		if(check_count == check_limit) break;
	}
	
	io_poll_time += (wtime() - this_time);
	return circ_load_chunk;
}

void cache_driver::clean_caches()
{
//	if(circ_free_chunk->get_sz() == 0)
		for(index_t i=0; i<num_chunks; i++)
		{
			if(cache[i]->status == PROCESSED)
			{
				//avoid enqueued again
				cache[i]->status = EVICTED;
				circ_free_chunk->en_circle(i);
			}
		}
}

