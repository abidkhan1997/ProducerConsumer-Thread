#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include<sys/types.h>
#include<sys/stat.h>
#include <fcntl.h>  
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

// File buffer size
#define FILE_BUFF_SIZE 4096
// Stack size
#define STACK_SIZE 100
// Max integer
#define MAX_INT 10000

char in_file[64];
char out_file[64];

// Global variables
int done = 0;
int minIntArray[10];
int arr_index = 0;
int total_int = 0;
int min = 0;

// conditional variables
pthread_cond_t full = PTHREAD_COND_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;
//pthread_cond_t main1 = PTHREAD_COND_INITIALIZER;

// Mutex lock
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// Stack structure
typedef struct Stack 
{
    int top;
    unsigned capacity;
    int* arr;
}Stack_t;

// Stack pointer
Stack_t *stack;

// function to create a stack of given capacity. It initializes size of
// stack as 0
void createStack()
{
    stack = (Stack_t*)malloc(sizeof(stack));
    stack->capacity = STACK_SIZE;
    stack->top = -1;
    stack->arr = (int*)malloc(stack->capacity * sizeof(int));
}

// Stack is full when top is equal to the last index
int isFull()
{
    return (stack->top == stack->capacity - 1);
}
 
// Stack is empty when top is equal to -1
int isEmpty()
{
    return (stack->top == -1);
}

// Function to add an item to stack.  It increases top by 1
void push(int item)
{
    if (isFull())
        return;
    stack->arr[++stack->top] = item;
}
 
// Function to remove an item from stack.  
// It decreases top by 1
int pop()
{
    if (isEmpty())
        return -1;
    return stack->arr[stack->top--];
}

void removeStack()
{
	free(stack->arr);
	free(stack);
}

// Producer thread
void *Producer(void *arg)
{    
	int fd = (*(int*)&arg);

	int i = 0;
	ssize_t len = 0;
	off_t offset = 0;
	char *buffer = (char *) malloc(1);
	long int integer = 0;

	// Reading file
	while((len = pread(fd, buffer, sizeof(buffer), offset)) != 0)
	{
		if(pthread_mutex_lock(&mutex) != 0)
		{
			printf("Failed to mutex lock in Producer thread\n");
		}
	
		if(buffer[0] != ' ' && (buffer[0] != '\n'))
		{
			integer *= 10;
			integer += (buffer[0] - '0');	
		}
		else
		{	// Push integers in stack
			push(integer);
			integer = 0;
		}
		
		// Check stack is full
		if(isFull())
		{
			pthread_cond_wait(&full, &mutex);
			sched_yield();
		}

		// Release lock
		if(pthread_mutex_unlock(&mutex) != 0)
		{
			printf("Failed to unlock mutex in producer thread\n");
		}
			
		// Send signal to consumer thread
		pthread_cond_signal(&empty);
	
		offset++;

		// Terminate thread
		if(total_int == MAX_INT+1)
		{
			free(buffer);
			pthread_exit(NULL);
		}		
	}
}

// Consumer thread
void *Consumer(void *arg)
{
	while(1)
	{
		// aquire lock
		if(pthread_mutex_lock(&mutex) != 0)
		{
			printf("Failed to mutex lock in consumer thread\n");
		}

		if(isEmpty() == -1)
		{            
			sched_yield();
			pthread_cond_wait(&empty, &mutex);
		}    
		// Pop up integers from stack
		else if (isFull() == 1)
		{
			int tmp = pop();
			int p = tmp;

			while(p != -1)
			{
				p = pop();     
				if((p < tmp) && (p != -1))
					tmp = p;
				total_int++;
			}
			minIntArray[arr_index++] = tmp;

			// Finding minimum int every 1000 integers
			if((total_int % 1000) == 0)
			{
				int x;
				min = minIntArray[0];
				for(x = 1; x < 10; x++)
				{
					if(minIntArray[x] < min)
						min = minIntArray[x];
				}

				done = 1;
				arr_index = 0;
				usleep(500);
			}
		}
          
		// Mutex unlock
		if(pthread_mutex_unlock(&mutex) != 0)
			printf("Failed to unlock mutex in consumer thread\n");
   		
		// Send signal to producer thread    
		pthread_cond_signal(&full);  
		
		// Terminating thread
		if(total_int == MAX_INT+1)
		{
			pthread_exit(NULL);
		}            
	}    
}

// Main thread
void *mainThread(void *arg)
{
	pthread_t ptid, ctid;

	int disp = 0;
	off_t off = 0;
	char buf[128];
	memset(buf, '\0', 128);

	// Open file
	long fd = open(in_file, O_RDONLY); 
      
    	if (fd < 0) 
    	{
        	perror("open");
        	exit(1);
    	}

	long fd1 = open(out_file, O_RDWR|O_CREAT, 0777); 
    	if (fd1 < 0) 
    	{
        	perror("open");
        	exit(1);
    	}

	// Create threads
	pthread_create(&ptid, NULL, Producer, (void*)fd);
	sleep(1);
	pthread_create(&ctid, NULL, Consumer,NULL);

	// Displaying minimum number every 1000 integers
	while(1)
	{
		if(done == 1)
		{
			disp += 1000;
			sprintf(buf, "Minimum integer after %d integers =\t%d\n", disp, min);
			pwrite(fd1, buf, sizeof(buf), off); 
			printf("%s\n", buf);
			done = 0;
			off += strlen(buf);
			memset(buf, '\0', 128);
		}
		else if(total_int == MAX_INT)
		{
			total_int++;
			pthread_exit(NULL);
		}
	}

	// Waiting for exit both threads
	pthread_join(ptid, NULL);
	pthread_join(ctid, NULL);

	// Close file descriptor
	if (close(fd) < 0) 
    	{
        	perror("close");
        	exit(1);
    	} 

	if (close(fd1) < 0) 
    	{
        	perror("close");
        	exit(1);
    	} 
}

// Main program entry point
int main(int argc, char **argv)
{    
	if(argc != 3)
	{
		printf("Please provide enough argument\n");
		return 0;
	}

	memset(in_file, '\0', 64);
	memset(out_file, '\0', 64);

	strcpy(in_file, argv[1]);
	strcpy(out_file, argv[2]);

	pthread_t tid;
          
	createStack();

	// create main thread
	pthread_create(&tid, NULL, mainThread, NULL);

	// waiting for terminate main thread
	pthread_join(tid, NULL);

	removeStack();

	return 0;
}
