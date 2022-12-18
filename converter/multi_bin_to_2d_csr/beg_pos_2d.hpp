#include "bin_struct_reader.h"
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include "wtime.h"
#include "util.hpp"

template<typename index_t, typename vertex_t, typename file_vertex_t>
void beg_pos_2d (
		vertex_t *row_ranger,
		vertex_t *col_ranger,
		const char *prefix,
		const int file_count,
		const int num_thds,
		const vertex_t vert_count,//max_vert + 1
		const int num_rows,
		const int num_cols
){
	printf("\n\n\n===========\nBeg_pos computation\n------\n");
	
	//alloc beg_pos array
	/*degree记录每一个col下的方块内各个点的出度，并且按照csr的方式累加例如:
	degree[0] = new index_t[vertex_count_of_current_row]
	degree[1] = new index_t[vertex_count_of_current_row]
	degree[2] = new index_t[vertex_count_of_current_row]
	degree[3] = new index_t[vertex_count_of_current_row]
	index_t里，每一个下表都代表着当前col下的某个vertex_id，以及它当前idx下出度的累积
	*/
	index_t **degree = new index_t*[num_rows*num_cols];
	for(int i = 0; i < num_rows*num_cols; i ++)
	{
		int my_row = i / num_cols;
		// row_count 记录着当前row的vertex个数。
		// 虽然这里涉及了多个block，但是每个block都只记录了自己所在row的vertex_count有点奇怪，以下是例子:
		/*
		part-0-0 row vert-count: 40656 len(degree[0])=40656
		part-0-1 row vert-count: 40656 len(degree[1])=40656
		part-0-2 row vert-count: 40656 len(degree[2])=40656
		part-0-3 row vert-count: 40656 len(degree[3])=40656
		part-1-0 row vert-count: 280949 len(degree[4])=280949
		part-1-1 row vert-count: 280949 len(degree[5])=280949
		part-1-2 row vert-count: 280949 len(degree[6])=280949
		part-1-3 row vert-count: 280949 len(degree[7])=280949
		part-2-0 row vert-count: 3004604 len(degree[8])=3004604
		part-2-1 row vert-count: 3004604 len(degree[9])=3004604
		part-2-2 row vert-count: 3004604 len(degree[10])=3004604
		part-2-3 row vert-count: 3004604 len(degree[11])=3004604
		part-3-0 row vert-count: 38326021 len(degree[12])=38326021
		part-3-1 row vert-count: 38326021 len(degree[13])=38326021
		part-3-2 row vert-count: 38326021 len(degree[14])=38326021
		part-3-3 row vert-count: 38326021 len(degree[15])=38326021
		*/
		index_t row_count = row_ranger[my_row+1]-row_ranger[my_row];
		degree[i] = new index_t[row_count];
		memset(degree[i], 0, sizeof(index_t)*row_count);
		printf("part-%d-%d row vert-count: %ld\n", my_row, i%num_cols, row_count);
	}
	
	//check if beg_pos is already computed
	bool is_beg_done = true;
	char filename[256];
	struct stat st;
	for(int i = 0; i < num_rows; i++)
	{
		for(int j = 0; j < num_cols; j++)
		{
			sprintf(filename, "%s_beg.%d_%d_of_%dx%d.bin", 
					prefix, i, j, num_rows, num_cols);
			if(stat(filename, &st)!=0)
			{
				is_beg_done = false;
				printf("%s is not found\n", filename);
				break;
			}

			if(st.st_size != (row_ranger[i+1] - row_ranger[i] + 1) * sizeof(index_t))
			{
				is_beg_done = false;
				printf("%s is not found\n", filename);
				break;
			}
			
			printf("%s found\n", filename);
		}
	}
	
	if(is_beg_done == true)
	{
		printf("beg_pos is already computed\n");
		return;
	}

	double tm = wtime();
#pragma omp parallel \
	num_threads(num_thds)
	{
		int tid = omp_get_thread_num();
		int step = file_count/num_thds;
		int remainder = file_count - num_thds * step;
		int my_file_beg;

		if(tid < remainder) 
		{
			step++;
			my_file_beg = tid * step;
		}
		else my_file_beg = tid * step + remainder;

		int my_file_end = my_file_beg + step;
		// for(int i = 0; i < num_thds; i++)
		// {
		//     if(tid == i)
		//         printf("thd-%d: %d ~ %d\n", i, my_file_beg, my_file_end);
		//#pragma omp barrier
		// }
		 char filename[256];

		 while(my_file_beg < my_file_end)
		 {
			 sprintf(filename, "%s-%05d.bin", prefix, my_file_beg);
			 if(tid==0) 
				 printf("Processing %s, %lf seconds\n", filename, wtime()-tm);

			 bin_struct_reader<file_vertex_t, index_t>
				 *inst = new bin_struct_reader<file_vertex_t, index_t>
				 ((const char *)filename);

			 for(index_t i = 0; i < inst->num_edges; i++)
			 {
				 vertex_t src = (vertex_t) inst->edge_list[i].src;
				 vertex_t dest= (vertex_t) inst->edge_list[i].dest;

				 int my_row = aligned_par<vertex_t, index_t>
					 (row_ranger, num_rows, src);

				 int my_col = misaligned_col<vertex_t, index_t>
					(col_ranger,  num_cols, dest, my_row);
				
				 index_t *my_degree = degree[my_row * num_cols + my_col];
				 //这里很好理解，举个例子:
				 /*	 
				 	part-0-0 row vert-count: 40656 len(degree[0])=40656
					part-0-1 row vert-count: 40656 len(degree[1])=40656
					part-0-2 row vert-count: 40656 len(degree[2])=40656
					part-0-3 row vert-count: 40656 len(degree[3])=40656
					part-1-0 row vert-count: 280949 len(degree[4])=280949
					part-1-1 row vert-count: 280949 len(degree[5])=280949
					part-1-2 row vert-count: 280949 len(degree[6])=280949
					part-1-3 row vert-count: 280949 len(degree[7])=280949
					根据上面的信息，row_ranger[1] = 40656, row_ranger[2] = 321605
					col_ranger[1] = 12800
					col_ranger[2] = 22400
					col_ranger[3] = 28600
					col_ranger[4] = 40656
					col_ranger[5] = 0
					col_ranger[6] = 40657
					col_ranger[7] = 120456
					col_ranger[8] = 260949
					col_ranger[9] = 321605
					当index_t *my_degree = degree[1 * 4 + 4] = degree[8]时
					src很可能是300420,那么src-row_ranger[my_row] == 300420 - 40656 = 259767
					那么my_degree + 259767就对应到它所在的vertex_id=300420在它所在的row1的位置上。
					因为degree[4~7]的len最大是280949，所以需要减去row_ranger来计算它的偏移值。
				 */
				 __sync_fetch_and_add(my_degree + 
						 src - row_ranger[my_row], 1);//这里记录的是出度
			 }

			 delete inst;
			 my_file_beg++;
		 }
	}

	printf("Finished count degree\nStart compute beg_pos\n");

	//use degree to compute inclusive beg_pos
	//for exclusive beg_pos, we add a 0 ahead
#pragma omp parallel num_threads(num_rows * num_cols)
		{
			//线程数是num_rows*num_cols正好是切分方块的数量的数量
			int tid = omp_get_thread_num();
			int my_row = tid / num_cols;
			for(index_t i = 1; i < row_ranger[my_row+1] - row_ranger[my_row];
						i++)
			{
				//累积的出度，csr的模式。不过如果是同一行里的几个线程。
				degree[tid][i] += degree[tid][i-1];	
			}
		}
	
	printf("Dump beg_pos to disk\n");

	for(int i = 0; i < num_rows; i++)
	{
		for(int j = 0; j < num_cols; j++)
		{
			sprintf(filename, "%s_beg.%d_%d_of_%dx%d.bin", 
					prefix, i, j, num_rows, num_cols);
			FILE *fd = fopen(filename, "wb");
			assert(fd != NULL);

			//write the beg_pos of the first vertex, -- 0.
			index_t init_beg[1]; init_beg[0] = 0;
			fwrite(init_beg, sizeof(index_t), 1, fd);
			fwrite(degree[i*num_cols + j], sizeof(index_t), 
					row_ranger[i+1] - row_ranger[i], fd);
			fclose(fd);
			printf("row-%d-col-%d, edgecount: %ld\n", i, j, 
					degree[i*num_cols + j]
					[row_ranger[i+1] - row_ranger[i]-1]);
		}
	}
	

	//dealloc arrays
	for(int i = 0; i < num_rows; i++)
		for(int j = 0; j < num_cols; j++)
			delete degree[i*num_cols + j];
	
	delete[] degree;
	return ;
}
