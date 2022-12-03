#ifndef __CIRCLE__
#define __CIRCLE__

#include "util.h"
#include "comm.h"
#include <atomic>

class circle
{
	public:
		int *array;
		int size;
		int head;
		int tail;
		volatile int lock_head;
		volatile int lock_tail;

		volatile std::atomic<int> num_elem;


	public: 
		circle(){};
		circle(int size);
		~circle();

	public:
		int en_circle(int id);
		int en_circle_v(int id);
		int de_circle();
		int de_circle_v();
		int get_sz();
		bool is_full();
		bool is_empty();
		void reset_circle();
};

#endif
