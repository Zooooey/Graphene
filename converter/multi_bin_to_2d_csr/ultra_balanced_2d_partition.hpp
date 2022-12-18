#include "bin_struct_reader.h"
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include "wtime.h"
#include "util.hpp"

template<typename index_t, typename vertex_t, typename file_vertex_t>
void ultra_balanced_2d_partition (
		vertex_t* &row_ranger,
		vertex_t* &col_ranger,
		const char *prefix,
		const int file_count,
		const int num_thds,
		const vertex_t vert_count,//max_vert + 1
		const index_t edge_count, 
		const int num_rows,
		const int num_cols
){
	printf("\n\n\n===========\nUltra balanced computation\n------\n");
	//alloc balanced partition ranger_beg matrix
	row_ranger = new vertex_t[num_rows + 1];
	//列ranger居然把行的数量囊括进来了，变成了(num_cols+1)*num_rows
	col_ranger = new vertex_t[(num_cols + 1) * num_rows];
	
	
	char name[256];	
	sprintf(name, "%s_beg-%dx%d-row-ranger.bin", prefix, num_rows, num_cols);
	FILE *file = fopen(name, "rb");
	size_t ret = 0;	
	if(file != NULL)
	{
		printf("row-ranger file is found\n");
		ret = fread(row_ranger, sizeof(vertex_t), num_rows + 1, file);
		assert(ret == num_rows + 1);
		fclose(file);
	}

	sprintf(name, "%s_beg-%dx%d-col-ranger.bin", prefix, num_rows, num_cols);
	file = fopen(name, "rb");

	if(file != NULL)
	{
		printf("col-ranger file is found\n");
		ret = fread(col_ranger, sizeof(vertex_t), (num_cols + 1) * num_rows, file);
		assert(ret == (num_cols + 1) * num_rows);
		fclose(file);

		printf("Will not do ultra-balanced partition again\n");
		return;
	}
	
	printf("No precomputed ultra balanced partition, we need to compute\n");
	memset(row_ranger, 0, sizeof(vertex_t)*(num_rows + 1));
	memset(col_ranger, 0, sizeof(vertex_t)*(num_cols + 1) * num_rows);

	//alloc beg_pos array
	//=========begin 这段代码主要填充outdegree记录每个点的出度值(其余细节可以不看了) ================
	index_t *outdegree = new index_t[vert_count];
	memset(outdegree, 0, sizeof(index_t)*vert_count);
		
	printf("Degree data struct allocated\n");
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
				 __sync_fetch_and_add(outdegree + src, 1);
			 }

			 delete inst;
			 my_file_beg++;
		 }
	}

	printf("Finished count outdegree\nStart compute inclusive out beg_pos.\n");
	//=========end 这段代码主要填充outdegree记录每个点的出度值(其余细节可以不看了) ================
	printf("And search through to find balanced row partition\n");
	
	//init beg of the first row	
	//边的数量除以num_rows，很容易理解到每行平均分到多少边。这个num_rows来自输入，例如1x1时，num_rows==1
	index_t row_par_step = (edge_count - 1) / num_rows + 1;
	//如果num_rows==1，这里居然变成了除以2
	int curr_row = 0; 
	row_ranger[curr_row ++] = 0;
	for(index_t i = 1; i < vert_count; i ++)
	{
		//这里开始构造csr idx了，每个outdegree时前一个点outdegree的累加
		outdegree[i] += outdegree[i - 1];	
		/*按照num_rows==1的例子，这里curr_row*row_par_step实际上是边数量的一半，
		因此这个outdegree实际上在找一个vertex_id，这个vertex_id之前的所有出边加起来，都没有边数量的一半多*/
		if((outdegree[i - 1] <= (curr_row * row_par_step)) &&
			(outdegree[i]  >= (curr_row * row_par_step)))
		{
			//row_ranger里记录的是"vertex_id" row_ranger作为数组，记录了当前row里都保存了哪些vertex_id
			row_ranger[curr_row] = i - 1;
			printf("curr_row-%d: %u ~ %u\n", curr_row
					,row_ranger[curr_row - 1], row_ranger[curr_row]);
			curr_row ++;
			if(curr_row == num_rows) break;
		}
	}
	/*这里是为了解决可能的遗漏，按照我们的1x1来看，row_ranger[1] == 41652228
	由于num_rows只有1，所以最后再对	row_ranger[num_rows] = vert_count = 41652230
	这就是为什么从输出的日志来看，一开始row_ranger[1] == 41652228，后来又变成了41652230
	*/
	row_ranger[num_rows] = vert_count;
	printf("curr_row-%d: %u ~ %u\n", curr_row
			,row_ranger[curr_row - 1], row_ranger[curr_row]);
	
	printf("Row-wise 1D partition is balanced and completed\n");
	printf("Working on column-wise 2D partition\n");	
	
	//=========begin 这段代码主要填充indegree记录每个点的入度值(其余细节可以不看了) ================
	//这里需要注意一点：这个indegree是个二维数组，第一维是行，第二维是自己所在行的点的入度。
	index_t **indegree = new index_t*[num_rows];
	for ( int i =  0; i < num_rows; i ++ )
	{
		indegree[i] = new index_t[vert_count];
		memset(indegree[i], 0, sizeof(index_t) * vert_count);
	}

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
				 //根据前面的说法，在构造完row_ranger后，只要给vertex_id，就可以知道这个vertex在哪个row了。
				 int my_row = aligned_par<vertex_t, index_t>
					 	(row_ranger, num_rows, src);
				 vertex_t dest= (vertex_t) inst->edge_list[i].dest;
				 index_t *my_degree = indegree[my_row];
				 __sync_fetch_and_add(my_degree + dest, 1);
			 }

			 delete inst;
			 my_file_beg++;
		 }
	}
	//=========end 这段代码主要填充indegree记录每个点的入度值(其余细节可以不看了) ================

	//Iterate through each row to find the balanced column partition
	//init beg of the first column	
	//!!!Assuming row is partitioned quite balance!!!
	//按照1x1的例子的话，这里还是除以2
	/*按照8x8的例子，这里和row_ranger的计算有一定区别，row_ranger计算时使用的时num_rows +1，因此得到的结果是9，也就是边的数量除以9。但是到col这里，变成了8*8+1 = 65,也就是边被分成了65份。 
	*/
	index_t col_par_step = (edge_count - 1 ) / (num_rows * num_cols) + 1;
	for (int n = 0; n < num_rows; n ++)
	{
		/*行被切成8行后，列再切的话，一个方块就被切成8*8的多个小方块了。那么n*(num_cols)则是:
		0*(8+1)
		1*(8+1)
		2*(8+1)
		*/

		col_ranger[n * (num_cols + 1)] = 0;
		int curr_col = 1; 
		for(index_t i = 1; i < vert_count; i ++)
		{
			//这个操作就是CSR的idx，不过这里用的不是出度，变成入度了。并且只统计所在row的入度。
			indegree[n][i] += indegree[n][i - 1];
			//底下的逻辑和分row的逻辑相似，但是在col这里用的是入度，而不是出度。
			if((indegree[n][i - 1] <= (curr_col * col_par_step)) &&
				(indegree[n][i]  >= (curr_col * col_par_step)))
			{
				//这里和row_ranger一样，记录col的区域的终点。注意col_ranger
				col_ranger[n * (num_cols + 1) + curr_col] = i - 1;
				printf("row-%d-curr-col-%d: %u ~ %u\n", n, curr_col-1,
					col_ranger[n * (num_cols + 1) + curr_col - 1], 
					col_ranger[n * (num_cols + 1) + curr_col]);
				curr_col ++;
				if(curr_col == num_cols) break;
			}
		}
		col_ranger[n * (num_cols + 1) + num_cols] = vert_count;
		printf("row-%d-curr-col-%d: %u ~ %u\n", n, curr_col-1,
				col_ranger[n * (num_cols + 1) + curr_col - 1], 
				col_ranger[n * (num_cols + 1) + curr_col]);
	}
	
	//dump the row_ranger and col_ranger to disk -- checkpointing
	sprintf(name, "%s_beg-%dx%d-row-ranger.bin", prefix, num_rows, num_cols);
	file = fopen(name, "wb");
	ret = fwrite(row_ranger, sizeof(vertex_t), num_rows + 1, file);
	assert(ret == num_rows + 1);
	fclose(file);
	
	sprintf(name, "%s_beg-%dx%d-col-ranger.bin", prefix, num_rows, num_cols);
	file = fopen(name, "wb");
	ret = fwrite(col_ranger, sizeof(vertex_t), (num_cols + 1) * num_rows, file);
	assert(ret == (num_cols + 1) * num_rows);
	fclose(file);
	return ;
}
