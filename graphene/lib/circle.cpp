#include "circle.h"
#include <iostream>
#include <sstream>
using namespace std;

inline void lock(volatile int &flag)
{
	while(!__sync_bool_compare_and_swap(&flag, 0, 1)){}
}

inline void unlock(volatile int &flag)
{
	flag = 0;
}

circle::circle(int size):size(size)
{
	head = 0;
	tail = 0;

	lock_head = 0;
	lock_tail = 0;
	num_elem = 0;

	array = new int[size];
}

circle::~circle()
{
	delete[] array;
}

int circle::en_circle_v(int id)
{
	int ret = -1;
	lock(lock_tail);

	if(num_elem != size)
	{
		//__sync_fetch_and_add(&num_elem, 1);	
		ret = 0;
		array[tail] = id;
		tail = (tail + 1) % size;
		//__sync_fetch_and_add(&num_elem, 1);	
		int old_num = num_elem;
		//__sync_add_and_fetch(&num_elem, 1);	
		num_elem++;
		std::stringstream stream; // #include <sstream> for this
		stream<<"	en_circle====>old_num:"<<old_num<<" new_num:"<<num_elem<<" tid:"<<omp_get_thread_num()<<endl;
		std::cout << stream.str();
	}
	unlock(lock_tail);
	return ret;
}
int circle::en_circle(int id)
{
	int ret = -1;
	lock(lock_tail);

	if(num_elem != size)
	{
		//__sync_fetch_and_add(&num_elem, 1);	
		ret = 0;
		array[tail] = id;
		tail = (tail + 1) % size;
		//__sync_fetch_and_add(&num_elem, 1);	
		//__sync_add_and_fetch(&num_elem, 1);	
		num_elem++;
	}
	unlock(lock_tail);
	return ret;
}


int circle::de_circle_v()
{
	int page_id = -1;
	
	lock(lock_head);
	if(num_elem != 0)
	{
		page_id = array[head];
		head = (head + 1) % size;
		int old_num = num_elem;
		//__sync_fetch_and_sub(&num_elem, 1);
		num_elem--;
		std::stringstream stream; // #include <sstream> for this
		stream<<"	de_circle====>old_num:"<<old_num<<" new_num:"<<num_elem<<" tid:"<<omp_get_thread_num()<<endl;
		std::cout << stream.str();
	}

	unlock(lock_head);
	return page_id;
}

int circle::de_circle()
{
	int page_id = -1;
	
	lock(lock_head);
	if(num_elem != 0)
	{
		page_id = array[head];
		head = (head + 1) % size;
		//__sync_fetch_and_sub(&num_elem, 1);
		num_elem--;
	}

	unlock(lock_head);
	return page_id;
}

int circle::get_sz()
{
	return num_elem;
}

void circle::reset_circle()
{
	head = tail;
	num_elem = 0;
	lock_head = 0;
	lock_tail = 0;
}

bool circle::is_full()
{
	return num_elem == size;
}

bool circle::is_empty()
{
	return num_elem == 0;
}
