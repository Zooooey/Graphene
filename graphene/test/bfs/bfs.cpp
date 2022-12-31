#include "cache_driver.h"
#include "IO_smart_iterator.h"
#include <stdlib.h>
#include <sched.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
//#include <asm/mman.h>
#include <linux/mman.h>
#include "pin_thread.h"
#include "get_vert_count.hpp"
#include "get_col_ranger.hpp"
#include <set>
#include "/home/ccy/Develop/GraphTools/COrder/Binning.hpp"
using namespace std;

#define CACHE_PATH "/home/ccy/data_set/twitter7/twitter7_FAM_GRAPH/bin_order.cache"
#define CACHE_RATIO 0

inline bool is_active
(index_t vert_id,
sa_t criterion,
sa_t *sa, sa_t *prior)
{
	return (sa[vert_id]==criterion);
//		return true;
//	else 
//		return false;
}


int main(int argc, char **argv) 
{
	std::cout<<"Format: /path/to/exe " 
		<<"#row_partitions #col_partitions thread_count "
		<<"/path/to/beg_pos_dir /path/to/csr_dir "
		<<"beg_header csr_header num_chunks "
		<<"chunk_sz (#bytes) concurr_IO_ctx "
		<<"max_continuous_useless_blk ring_vert_count num_buffs source\n";

	if(argc != 15)
	{
		fprintf(stdout, "Wrong input\n");
		exit(-1);
	}

	//Output input
	for(int i=0;i<argc;i++)
		std::cout<<argv[i]<<" ";
	std::cout<<"\n";

	const int row_par = atoi(argv[1]);
	const int col_par = atoi(argv[2]);
	const int NUM_THDS = atoi(argv[3]);
	const char *beg_dir = argv[4];
	const char *csr_dir = argv[5];
	const char *beg_header=argv[6];
	const char *csr_header=argv[7];
	const index_t num_chunks = atoi(argv[8]);
	const size_t chunk_sz = atoi(argv[9]);
	const index_t io_limit = atoi(argv[10]);
	const index_t MAX_USELESS = atoi(argv[11]);
	const index_t ring_vert_count = atoi(argv[12]);
	const index_t num_buffs = atoi(argv[13]);
	vertex_t root = (vertex_t) atol(argv[14]);

	//TODO:用一个bool数组确定哪些点在cache哪些不在。
	
	assert(NUM_THDS==(row_par*col_par*2));
	sa_t *sa = NULL;
	index_t *comm = new index_t[NUM_THDS];
	vertex_t **front_queue_ptr;
	index_t *front_count_ptr;
	vertex_t *col_ranger_ptr;
	
	const index_t vert_count=get_vert_count
		(comm, beg_dir,beg_header,row_par,col_par);
	get_col_ranger(col_ranger_ptr, front_queue_ptr,
			front_count_ptr, beg_dir, beg_header,
			row_par, col_par);
	
	sa=(sa_t *)mmap(NULL,sizeof(sa_t)*vert_count,
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB , 0, 0);
	//| MAP_HUGETLB , 0, 0);

	cout<<"Init static cache...."<<endl;
	//mark:初始化填充一个cache
	set<uint32_t>** static_cache = new set<uint32_t>*[vert_count];
	for(uint32_t i =0;i<vert_count;i++){
		static_cache[i] = nullptr;
	}
	//fake:除了0，所有的点都只有一个假后继节点，就是自己。

	if(CACHE_RATIO != 0){
		double cache_read = wtime();
		CacheMap *cache_map =
            BinReader::read_bin_cache(CACHE_PATH, CACHE_RATIO, vert_count);
		cout<<"Construct Cache Time:"<<wtime()-cache_read<<" seconds(s)"<<endl;
	}
	

	/*for(uint32_t i=1;i<vert_count;i++){
		static_cache[i] = new set<uint32_t>();
		static_cache[i]->insert(i);
	}*/

	if(sa==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}

	const index_t vert_per_blk = chunk_sz / sizeof(vertex_t);
	if(chunk_sz&(sizeof(vertex_t) - 1))
	{
		std::cout<<"Page size wrong\n";
		exit(-1);
	}
	sa_t *sa_dummy=NULL;

	char cmd[256];
	sprintf(cmd,"%s","iostat -x 1 -k > iostat_bfs.log&");
	std::cout<<cmd<<"\n";
	
	int *semaphore_acq = new int[1];
	int *semaphore_flag = new int[1];

	//	gpu_semaphore[0]=1;
	//omp_lock_t gpu_semaphore;
	//omp_init_lock(&gpu_semaphore);
	//0 1 2 3 4 5 6 7 8 9 10 11 12 13 28 29 30 31 32 33 34 35 36 37 38 39 40 41
	//14 15 16 17 18 19 20 21 22 23 24 25 26 27 42 43 44 45 46 47 48 49 50 51 52 53 54 55
	//int core_id[8]={0, 2, 4, 6, 14, 16, 18, 20};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 14, 16, 18, 20, 22, 24, 26, 42};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 30, 32, 34, 36, 38, 40, 1, 3};

	//0 1 2 3 4 5 12 13 14 15 16 17
	//6 7 8 9 10 11 18 19 20 21 22 23
//	int socket_one[12]={0, 6, 1, 7, 2, 8, 3, 9, 4, 10, 5, 11};
//	int socket_two[12]={12, 18, 13, 19, 14, 20, 15, 21, 16, 22, 17, 23};
	
	int socket_one[12]={0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17};
	int socket_two[12]={6, 7, 8, 9, 10, 11, 18, 19, 20, 21, 22, 23};
	memset(sa, INFTY, sizeof(sa_t)*vert_count);
	cout<<"Threads Num:"<<NUM_THDS<<" to build IO_smart_iterator..."<<endl;
	sa[root] = 0;
	IO_smart_iterator **it_comm = new IO_smart_iterator*[NUM_THDS];
	
	double tm = 0;
#pragma omp parallel \
	num_threads (NUM_THDS) \
	shared(sa, root, comm, front_queue_ptr, \
			front_count_ptr, col_ranger_ptr, static_cache)
	{
		std::stringstream travss;
		travss.str("");
		travss.clear();

		std::stringstream fetchss;
		fetchss.str("");
		fetchss.clear();

		std::stringstream savess;
		savess.str("");
		savess.clear();

		sa_t level = 0;
		int tid = omp_get_thread_num();
		//线程分组，每个组都有两个线程
		int comp_tid = tid >> 1;
		comp_t *neighbors;
		index_t *beg_pos;
		//if(tid < 16) 
		//	pin_thread_socket(socket_one, 12);
		//else
		//	pin_thread_socket(socket_two, 12);

		index_t prev_front_count = 0;
		index_t front_count = 0;
		
		if((tid&1) == 0) 
		{
			IO_smart_iterator *it_temp = 
				new IO_smart_iterator(
						front_queue_ptr,
						front_count_ptr,
						col_ranger_ptr,
						comp_tid,comm,
						row_par,col_par,	
						beg_dir,csr_dir, 
						beg_header,csr_header,
						num_chunks,
						chunk_sz,
						sa,sa_dummy,beg_pos,
						num_buffs,
						ring_vert_count,
						MAX_USELESS,
						io_limit,
						&is_active);
			it_temp->set_static_cache(cache_map);
			it_temp->init_cache_hit_list(cache_map->get_cached_edges_count());
		
			//每个线程都有一个queue，每个queue第一个vertex_id都是root
			front_queue_ptr[comp_tid][0] = root;
			front_count_ptr[comp_tid] = 1;

			prev_front_count = front_count_ptr[comp_tid];
			it_comm[tid] = it_temp;
			it_comm[tid]->is_bsp_done = false;
		}
#pragma omp barrier
		IO_smart_iterator *it = it_comm[(tid>>1)<<1];

		if(!tid) system((const char *)cmd);
		double convert_tm = 0;
		while(true)
		{
			front_count = 0;
			//- Framework gives user block to process
			//- Figures out what blocks needed next level
			if((tid & 1) == 0)
			{
				it -> io_time = 0;
				it -> wait_io_time = 0;
				it -> wait_comp_time = 0;
				it -> cd -> io_submit_time = 0;
				it -> cd -> io_poll_time = 0;
				it -> cd -> fetch_sz = 0;
			}

			double ltm=wtime();
			convert_tm=wtime();
			if((tid & 1) == 0)
			{
				 cout<<"again!"<<endl;
				it->is_bsp_done = false;
				if((prev_front_count * 100.0)/ vert_count > 2.0) 
					it->req_translator(level);
				else
				{
					it->req_translator_queue();
				}
			}
			else it->is_io_done = false;
#pragma omp barrier

			convert_tm=wtime()-convert_tm;
			if((tid & 1) == 0)
			{
				//working thread use a dead loop to retrieved
				while(true)
				{	
					int chunk_id = -1;
					double blk_tm = wtime();
					uint64_t debuging = 0;
					//=================handle cache begin==================
					/*原生逻辑在这个阶段只做了如下事情：
					1. 判断chunk对应的vert_id是不是我们需要处理的当前level的点(cache不需要考虑这个)
					2. 判断该邻居有没有被遍历过sa[nebr] == INFTY,若没有，设置它的sa[nebr] = level+1
					3. 把符合遍历的邻居节点推入front_queue里。
					*/
					if (it->vert_hit_in_cache_count!=0)
					{
						cout<<it->vert_hit_in_cache_count<<" verts hit in cache"<<endl;
						for(uint32_t index =0;index<it->vert_hit_in_cache_count;index++)
						{
							
						//	cout<<"vert hit in cache:"<<*iter<<endl;
							vertex_t nebr = static_cast<vertex_t>(it->vert_hit_in_cache[index]);
							
							if (sa[nebr] == INFTY)
							{
								sa[nebr] = (unsigned int)level + 1;
								if (front_count <= it->col_ranger_end - it->col_ranger_beg)
								{
									//cout<<"nebr:"<<nebr<<endl;
									it->front_queue[comp_tid][front_count] = nebr;
								}
								front_count++;
							}
						}
					}
					it->vert_hit_in_cache_count=0;
					//=================handle cache end==================
					//polling a loaded chunk from circle queue!
					while((chunk_id = it->cd->circ_load_chunk->de_circle())
							== -1)
					{
						if(it->is_bsp_done)
						{
							chunk_id = it->cd->circ_load_chunk->de_circle();
							break;
						}
					}
					it->wait_io_time += (wtime() - blk_tm);

					if(chunk_id == -1) break;
					//chunk是从迭代器it里的cd->cache拿到的。cd是cache_driver。或许我们可以改写cache_driver达到目的
					struct chunk *pinst = it->cd->cache[chunk_id];	
					index_t blk_beg_off = pinst->blk_beg_off;
					index_t num_verts = pinst->load_sz;
					vertex_t vert_id = pinst->beg_vert;
					//process one chunk
					while(true)
					{
						//vert_id is the id currently processing, level used to check bfs level.
						if(sa[vert_id] == (unsigned int)level)
						{
							index_t beg = beg_pos[vert_id - it->row_ranger_beg] 
								- blk_beg_off;
							index_t end = beg + beg_pos[vert_id + 1 - 
								it->row_ranger_beg]- 
								beg_pos[vert_id - it->row_ranger_beg];
							//possibly vert_id starts from preceding data block.
							//there by beg<0 is possible
							if(beg<0) beg = 0;

							if(end>num_verts) end = num_verts;
							for( ;beg<end; ++beg)
							{
								//这个nebr应该是邻居的意思，在这个范围内把邻居找出来，让sa[nebr]赋值level+1
								vertex_t nebr = pinst->buff[beg];//遍历过程的vertex_id从pinst->buff里得到。而pinst这里是一个chunk。关键需要改变pinst里的内容才行。
								//没被赋值过的都是INFTY，所以就赋值。
								if(sa[nebr] == INFTY)
								{
									sa[nebr]=(unsigned int)level+1;
									//为什么要判断front_count?
									if(front_count <= it->col_ranger_end - it->col_ranger_beg){
										//这里很好理解，每个comp_tid记录自己的front_queue，里面记录的是neighbor_id;
										it->front_queue[comp_tid][front_count] = nebr;
									}
									front_count++;
								}
							}
						}
						//TODO:这里把tid自己的cache_queue里的东西拿出来放入front_queue，然后清空
						//id是连续的，所以这里++
						++vert_id;
						
						//这里id超过row_ranger_end就终止循环
						if(vert_id >= it->row_ranger_end){
							 break;
						} else {
							
						}
						//vert_id - row_ranger_beg的含义是
						if(beg_pos[vert_id - it->row_ranger_beg]
								- blk_beg_off > num_verts) {
							break;
						}
						else {
						}
					}

					pinst->status = EVICTED;
					assert(it->cd->circ_free_chunk->en_circle(chunk_id)!= -1);
				}
				it->front_count[comp_tid] = front_count;
			}
			else
			{
				while(it->is_bsp_done == false)
				{
					it->load_key(level);
				}
			}
finish_point:	
			comm[tid] = front_count;
#pragma omp barrier
			front_count = 0;
			for(int i = 0 ;i< NUM_THDS; ++i)
				front_count += comm[i];
			if (!tid)cout<<"sum up front_count:"<<front_count<<endl;

			ltm = wtime() - ltm;
			if(!tid) tm += ltm;

#pragma omp barrier
			comm[tid] = it->cd->fetch_sz;
#pragma omp barrier
			index_t total_sz = 0;
			for(int i = 0 ;i< NUM_THDS; ++i)
				total_sz += comm[i];
			total_sz >>= 1;//total size doubled
			
			if(!tid) std::cout<<"@level-"<<(int)level
				<<"-font-leveltime-converttm-iotm-waitiotm-waitcomptm-iosize: "
				<<front_count<<" "<<ltm<<" "<<convert_tm<<" "<<it->io_time
				<<"("<<it->cd->io_submit_time<<","<<it->cd->io_poll_time<<") "
				<<" "<<it->wait_io_time<<" "<<it->wait_comp_time<<" "
				<<total_sz<<"\n"<<endl;
			
			if(front_count == 0 || level > 254) break;
			prev_front_count = front_count;
			front_count = 0;
			++level;
//#pragma omp barrier
			//if(!tid) std::cout<<"\n\n\n";
//#pragma omp barrier
		}
		if(!tid)system("killall iostat");

		if(!tid) std::cout<<"Total time: "<<tm<<" second(s)\n";

		if((tid & 1) == 0) delete it;
	}
	
	munmap(sa,sizeof(sa_t)*vert_count);
	delete[] comm;
	//	gpu_freer(keys_d);	
	return 0;
}
