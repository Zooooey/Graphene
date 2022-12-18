#include "bin_struct_reader.h"
#include <iostream>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include "wtime.h"

template<typename index_t, typename vertex_t, typename file_vertex_t>
void 
glb_vert_ranger(
		const char *prefix,
		const int file_count,
		const int num_thds,
		vertex_t  &glb_max_vert,
		vertex_t  &glb_min_vert,
		index_t &glb_edge_count
){
	printf("\n\n\n===========\nVert-ranger computation\n------\n");

	char name[256];
	sprintf(name, "%s-glb-vert-edge-count.bin", prefix);
	FILE *file = fopen(name, "rb");
	size_t ret = 0;	
	bool is_config = true;
	
	if(file != NULL)
	{
		printf("glb-vert-edge-count file is found\n");
		ret = fread(&glb_min_vert, sizeof(vertex_t), 1, file);
		if(ret != 1)
		{
			perror("fread");
			is_config = false;
			printf("glb_min_vert: %u wrong, ret = %d\n", glb_min_vert, ret);
		}

		ret = fread(&glb_max_vert, sizeof(vertex_t), 1, file);
		if(ret != 1 || glb_min_vert >= glb_max_vert)
		{
			perror("fread");
			is_config = false;
			printf("glb_max_vert: %u wrong, ret = %d\n", glb_max_vert, ret);
		}

		ret = fread(&glb_edge_count, sizeof(index_t), 1, file);
		if(ret != 1 || glb_edge_count <= 0)
		{
			perror("fread");
			is_config = false;
			printf("glb_edge_count: %ld wrong, ret = %d\n", glb_edge_count, ret);
		}

		fclose(file);
	}else{
		is_config = false;
		printf("glb-vert-edge-count cannot open\n");
	}
	
	if(is_config)
	{
		printf("glb-vert-edge-count works\n");
		printf("min: %u, max:%u, total edge count: %ld\n", 
			glb_min_vert, glb_max_vert, glb_edge_count);
		return;
	}

	//这里记录每个线程处理的n个文件集合的统计信息，例如:comm
	index_t *comm_num_edges = new index_t[num_thds];
	vertex_t *comm_min_vert = new vertex_t[num_thds];
	vertex_t *comm_max_vert = new vertex_t[num_thds];

	double tm = wtime();
#pragma omp parallel \
	num_threads(num_thds)
	{
		int tid = omp_get_thread_num();
		//文件数量除以线程数，如果文件数是201，线程96。那么step=2
		int step = file_count/num_thds;
		//remainder是余数，剩9
		int remainder = file_count - num_thds * step;
		int my_file_beg;

		if(tid < remainder) 
		{
			step++;
			my_file_beg = tid * step;
		}
		else
			my_file_beg = tid * step + remainder;

		int my_file_end = my_file_beg + step;

		//tid=0时，[my_file_beg,end) = [0,3)
		//tid=1时，[3，6)
		//....
		//tid=8时, [24,27)


		//tid=9时,因为step=2，因此[18+9=27,27+2=29)
		//tid=10时,[29, 31)
		//总结：每个线程都有自己的一个区间，这个区间代表着什么？

		// for(int i = 0; i < num_thds; i++)
		// {
		//     if(tid == i)
		//         printf("thd-%d: %d ~ %d\n", i, my_file_beg, my_file_end);
		//#pragma omp barrier
		// }
		 index_t my_edge_count = 0;
		 vertex_t min_vert = (int)(1<<30);
		 vertex_t max_vert = 0;
		 char filename[256];

		//总结：因为每个线程处理的是自己的区间，加上我们处理的时候把一个原始二进制文件分成了多个小文件片。因此每个线程都打开对应数量自己的小文件片。
		 while(my_file_beg < my_file_end)
		 {

		     sprintf(filename, "%s-%05d.bin", prefix, my_file_beg);
		     if(tid==0) 
		         printf("Processing %s, %lf seconds\n", filename, wtime()-tm);

		     bin_struct_reader<file_vertex_t, index_t>
		         *inst = new bin_struct_reader<file_vertex_t, index_t>
		         ((const char *)filename);
		     inst->vert_ranger();

		     //由于每个线程不止读一个文件，因此min_vert和max_vert
		     if(min_vert > inst->min_vert) min_vert = inst->min_vert;
		     if(max_vert < inst->max_vert) max_vert = inst->max_vert;
		     
			 //累加本线程所有待处理文件的边总量
		     my_edge_count += inst->num_edges;
		     delete inst;
		     my_file_beg++;
		 }

		 comm_num_edges[tid] = my_edge_count;
		 comm_min_vert[tid] = min_vert;
		 comm_max_vert[tid] = max_vert;

	}

	glb_min_vert = INFTY_MAX;
	glb_max_vert = 0;
	glb_edge_count = 0;
	for(int i = 0; i < num_thds; i ++)
	{
		if(glb_min_vert > comm_min_vert[i]) glb_min_vert = comm_min_vert[i];
		if(glb_max_vert < comm_max_vert[i]) glb_max_vert = comm_max_vert[i];
		glb_edge_count += comm_num_edges[i];
	}
	printf("min: %u, max:%u, total edge count: %ld\n", 
			glb_min_vert, glb_max_vert, glb_edge_count);
	
	file=fopen(name, "wb");
	assert(file != NULL);
	assert(fwrite(&glb_min_vert, sizeof(vertex_t), 1, file) == 1);
	assert(fwrite(&glb_max_vert, sizeof(vertex_t), 1, file) == 1);
	assert(fwrite(&glb_edge_count, sizeof(index_t), 1, file) == 1);
	fclose(file);
}
