#include "bin_struct_reader.h"
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "wtime.h"
#include "util.hpp"

template<typename index_t, typename vertex_t, typename file_vertex_t>
void csr_2d_mem (
		vertex_t *row_ranger,
		vertex_t *col_ranger,
		const char *prefix,
		const int file_count,
		const int num_thds,
		const vertex_t vert_count,//max_vert + 1
		const int num_rows,
		const int num_cols
){
	printf("\n\n\n===========\nCSR computation\n------\n");
	//alloc beg_pos array
	index_t **beg_pos = new index_t*[num_rows*num_cols];
	vertex_t **csr = new vertex_t*[num_rows*num_cols];
		
	for(int tid = 0; tid < num_rows * num_cols; tid ++)
	{
		int my_row = tid / num_cols;
		index_t row_sz = row_ranger[my_row + 1] - row_ranger[my_row];
		beg_pos[tid] = new index_t[row_sz + 1];
		
		//reading beg_pos
		//reading beg_pos的目的是读取每个col块的csr格式的idx值（每个点相比于前一个点的出度累加）
		char filename[256];
		sprintf(filename, "%s_beg.%d_%d_of_%dx%d.bin", prefix, 
				my_row , tid % num_cols, num_rows, num_cols);
		FILE *fd = fopen(filename, "rb");

		assert(fd != NULL);
		assert(fread(beg_pos[tid], sizeof(index_t), row_sz + 1, fd) == row_sz + 1);
		fclose(fd);
		
		//从beg_pos的存储与信息可以知道
		csr[tid] = new vertex_t[beg_pos[tid][row_sz]];

		//Once beg_pos is loaded, we know the number edges for each par
		//sprintf(filename, "%s_csr.%d_%d_of_%dx%d.bin", prefix, 
		//		my_row , tid % num_cols, num_rows, num_cols);
		//int fd1 = open(filename, O_CREAT | O_RDWR, 00666);
		//assert(ftruncate (fd1, beg_pos[tid][row_sz]*sizeof(vertex_t)) == 0);
		//csr[tid] = (vertex_t *) mmap(NULL, 
		//		beg_pos[tid][row_sz]*sizeof(vertex_t),
		////		PROT_READ|PROT_WRITE,MAP_PRIVATE,fd1, 0);
		////MAP_PRIVATE: DO NOT DIRECTLY UPDATE THE MAPED FILE
		////MAP_SHARED: The updates of a file is visible to other processes
		//PROT_READ|PROT_WRITE,MAP_SHARED,fd1, 0);
		//close(fd1);
	}

	//for debug beg_pos -- correct
//	vertex_t front[9]={905712,  6376988,  7443019,  17863487,  17889768,  21314903,  22631496,  23324951,  31853342};
//	for(int i = 0; i < 9; i ++)
//	{
//		vertex_t ya = front[i];
//		int my_row = aligned_par<vertex_t, index_t>
//			(row_ranger, num_rows, ya);
//	
//		printf("outdegree-%d: ", ya);
//		for(int j = 0; j < 4; j ++)
//		{
//			printf("%ld ", beg_pos[my_row*num_cols + j][ya-row_ranger[my_row]+1] 
//						- beg_pos[my_row*num_cols + j][ya-row_ranger[my_row]]);
//		}
//		printf("\n");
//	}
//	exit(-1);


	printf("Data structure allocation finished\n");
	double tm = wtime();
#pragma omp parallel \
	num_threads(num_thds)
	{
		int tid = omp_get_thread_num();
		int step = file_count/num_thds;
		int remainder = file_count - num_thds * step;
		int const_file_beg;

		if(tid < remainder) 
		{
			step++;
			const_file_beg = tid * step;
		}
		else const_file_beg = tid * step + remainder;

		const int my_file_end = const_file_beg + step;
		char filename[256];
		
		 //Process the files in four rounds
		 vertex_t process_delta = vert_count;
		 for(vertex_t process_beg = 0; process_beg < vert_count; process_beg += process_delta)
		 {
			if(tid == 0) printf("Pass through file again\n\n\n");
#pragma omp barrier
			 vertex_t process_end = process_beg + process_delta;
			 int my_file_beg = const_file_beg;
			while(my_file_beg < my_file_end)
			{
				 sprintf(filename, "%s-%05d.bin", prefix, my_file_beg);
				 if(tid==0) 
					 printf("Processing %s, %lf seconds\n", filename, wtime()-tm);

				 bin_struct_reader<file_vertex_t, index_t>
					 *inst = new bin_struct_reader<file_vertex_t, index_t>
					 ((const char *)filename);

				 index_t processed_line = 1;
				 for(index_t i = 0; i < inst->num_edges; i ++)
				 {
					 if(i > processed_line && tid == 0)
					 {
						 printf("%lf%% processed, %lf seconds\n", 
								 (i*100.0)/inst->num_edges, wtime() - tm);
						 processed_line <<= 1;
					 }
					 vertex_t src = (vertex_t) inst->edge_list[i].src;
					 vertex_t dest= (vertex_t) inst->edge_list[i].dest;
					
					 //Each time, we only process a small portition
					 if(src < process_beg || src >= process_end) continue;
					 int my_row = aligned_par<vertex_t, index_t>
						 (row_ranger, num_rows, src);
						
					 int my_col = misaligned_col<vertex_t, index_t>
						 (col_ranger,  num_cols, dest, my_row);

					 int aligned_par = my_row * num_cols + my_col;
					 vertex_t *my_csr = csr[aligned_par];
					 index_t *my_beg_pos = beg_pos[aligned_par];

					 //printf("%ld - %ld\npar_step_row:%d\n", src, dest, par_step_row);
					 //printf("yaoffset:%d\n", my_offset[src % par_step_row]);
					 vertex_t beg_off = src - row_ranger[my_row];

					 //fast!
					 index_t curr_off = __sync_fetch_and_add(my_beg_pos	+ beg_off, 1);

					 //This takes majority of the time
					 my_csr[curr_off] = dest;
				 }
				 if(tid == 0) printf("\n\n");

				 delete inst;
				 my_file_beg++;
			 }
		 }
	}
	printf("Finished generating CSR file\n");
	
//	//for debug csr -- correct
//	vertex_t front[9]={905712,  6376988,  7443019,  17863487,  17889768,  21314903,  22631496,  23324951,  31853342};
//
//	printf("\n\n\n\n");
//	for(int i = 0; i < 9; i ++)
//	{
//		vertex_t ya = front[i];
//		int my_row = aligned_par<vertex_t, index_t>
//			(row_ranger, num_rows, ya);
//	
//		for(int j = 0; j < 4; j ++)
//		{
//			//because beg_pos is shifted ahead
//			for(int m = beg_pos[my_row*num_cols + j][ya-row_ranger[my_row]-1]; 
//					m < beg_pos[my_row*num_cols + j][ya-row_ranger[my_row]]; m ++)
//				printf("%u\n", csr[my_row * num_cols + j][m]);
//			//printf("%ld ", beg_pos[my_row*num_cols + j][ya-row_ranger[my_row]+1] 
//			//			- beg_pos[my_row*num_cols + j][ya-row_ranger[my_row]]);
//		}
//		//printf("\n");
//	}
//	printf("\n\n\n\n");
//	exit(-1);
	
	for(int i = 0; i < num_rows * num_cols; i ++)
	{
		printf("Parition %d: ", i);
		//for(int j = 0; j < beg_pos[i][par_step_row]; j++)
		for(int j = 0; j < 10; j++)
			printf("%u ", csr[i][j]);
		printf("\n");
		int my_row = i/num_cols;	
		index_t row_sz = row_ranger[my_row+1] - row_ranger[my_row];

		char filename[256];
		sprintf(filename, "%s_csr.%d_%d_of_%dx%d.bin", prefix, 
				my_row , i % num_cols, num_rows, num_cols);
		FILE *file = fopen(filename, "wb");
		assert(fwrite(csr[i], sizeof(vertex_t), beg_pos[i][row_sz], file) 
				== beg_pos[i][row_sz]);
		fclose(file);
	}

	return ;
}
