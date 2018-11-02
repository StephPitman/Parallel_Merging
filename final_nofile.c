#include <stdio.h>
#include <math.h>
#include <stdbool.h>
#include <mpi.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>


int ID = -1;

int logWritten =0;

void write_log (char *text ){
    FILE *logf;
    
    if (logWritten == 1){
    
        logf = fopen ("log1.txt", "a");
    }else{
        logf = fopen ("log1.txt", "w");
        logWritten = 1;
    }
    
    char buff[100];
    
    sprintf(buff,"<id:%d>", ID);
    
    fwrite (buff, 1, strlen(buff),logf);
    
    fwrite (text, 1, strlen(text),logf);
    
    fwrite ("\n", 1, 1,logf);
    
    fclose(logf);
    
}



long *generate_array( long size){
      
      
    long *array = malloc (sizeof(long)*size);
    
    
    long y = 0;
    for (long x = 0; x < size; x++){
        int t = rand() % 3;
        
        if (t == 0){
			t = rand() % 3;
            y+= (t+1);
           
        }
        
       array[x] = y;
        
        
        sprintf(buff, " generated array[%d]=%d",x,y);
        write_log (buff);
 //               
    }
    
 

    return array;
}


/*used wikipedia as reference
 *Inputs:
 *		-array of integers to be searched
 *		-size of the array
 *		-target value to find
 *Output:
 *		-the index of the value or closes value that is smaller than target
*/
long binary_search(long *array, long n, long target){
	long L=0;
	long R=n;
	long m;
	while(L<R){
		m=floor(L+(R-L)/2);
		
		
		if (array[m] < target){
			L=m+1;
		}else{
			R=m;
		}
	}
	return L;
}

/*used parallel merging pdf as reference
 *Inputs:
 *		-array of integers "A"
 *		-array of integers "B"
 *		-array of integers "C"
 *		-starting index of merging for array A
 *		-ending index of merging for array A
 *		-starting index of merging for array B
 *		-ending index of merging for array B
 *Output:
 *		-the array passed as C will be modified as merged A & B
*/
void serial_merge(long *a, long *b, long **c, long a_size, long size_b) {
	long i = 0;
	long j = 0;
	long k = 0;
	long b_j;
	long a_i;
	long n2 = a_size + size_b;
    
    *c=malloc(sizeof(long)*n2);
	while (k < n2 && i < a_size && j < size_b) {
		a_i = a[i];
		b_j = b[j];
		if (a_i <= b_j) {
			(*c)[k] = a_i;
			i++;
		} else {
			(*c)[k] = b_j;
			j++;
		}
		k++;
	}
	if (i >= a_size) {
		for (; j < size_b; j++) {
			b_j = b[j];
			(*c)[k] = b_j;
			j++;
			k++;
		}
	} else if (j >= size_b) {
		for (; i < a_size; i++) {
			a_i = a[i];
			(*c)[k] = a_i;
			i++;
			k++;
		}
	}

}

long load_array(FILE *file, long start, long end, long **a){
	long size = end - start;
    char buff[500];
	*a=malloc(sizeof(long)*(size));
	fseek(file,start,SEEK_SET);
	fread(*a,4,size,file);
	fclose(file);
    
    sprintf(buff,"array[0] =%d \n",(*a)[0] );
        write_log(buff);
    
    
    
	
	return size;
	
	
}




int main(int argc, char *argv[]){
	char *file1;
    
    
    
    
     write_log("started main");
	
    MPI_Status status;
	
    
    file1=argv[1];
	long n1=atol(argv[2]);
	long n2=atol(argv[3]);
    
    write_log( " parsed inputs");

	
	MPI_Init(&argc, &argv);
	int id,p;
	long *a;
	long *b;
	long *c;
	long *bigC;
	long n3 = n1+n2;
	
	
	
	MPI_Comm_rank (MPI_COMM_WORLD, &id);
	MPI_Comm_size (MPI_COMM_WORLD, &p);
  	ID = id;
	if (id==0){
		a=generate_array(n1);
		b=generate_array(n2);
		
		MPI_Bcast(&n1,1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(&n2,1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(a,n1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(b,n2,MPI_LONG,0,MPI_COMM_WORLD);
	}
	else{
		MPI_Bcast(&n1,1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(&n2,1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(a,n1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(b,n2,MPI_LONG,0,MPI_COMM_WORLD);
	}
	long i_0, i_1;  //start and finish of the segment A to merge
	long j_0, j_1; //start and finish of the segment B to merge

	int chunk_size=n1/p;
	long a_size, b_size;
	//0 cs 1cs
	//1cs+1 2cs
	//2cs+1 3cs
	if (id!=p-1){
		i_0=id*chunk_size;
		i_1=(id+1)*chunk_size;
        
        
            write_log("decided on indexes for first array");
     
		a_size=chunk_size;
		j_0=binary_search(b,n2,i_0);
		j_1=binary_search(b,n2,i_1);
		
        write_log("decided on indexes for 2nd array");
        
        
		b_size=j_1-j_0;
        
        write_log("merging chunk");
		serial_merge(a,b,&c,a_size, b_size);
        write_log("merged chunk");
        
	}else{
		i_0=id*chunk_size;
		i_1=n1-1;
		a_size=i_1-i_0;
        
        write_log("decided on indexes for first array");
       
        
		j_0=binary_search(b,n2,i_0);
		j_1=n2-1;
        
        write_log("decided on indexes for 2nd array");
        write_log("merging chunk");
		serial_merge(a,b,&c,a_size,b_size);
        write_log("merged chunk");
	}
	
	if (id==0){
        write_log ("receiving");
		bigC=malloc(sizeof(long)*n3);
		long offset = 0;
		for(int source=1;source<p;source++){
            write_log ("receiving proc");
            
          
			long rcv_size;
			MPI_Recv(&rcv_size,1,MPI_UNSIGNED_LONG,source,1,MPI_COMM_WORLD,&status);
			MPI_Recv(bigC+offset,rcv_size,MPI_UNSIGNED_LONG,source,2,MPI_COMM_WORLD,&status);
			offset+=rcv_size;
		}
        write_log ("received all");
		FILE *f;
		f = fopen(file1,"w");
		fwrite(bigC,4,offset,f);
		fclose(f);

	}else{
        write_log ("sending"); 
		MPI_Send(&a_size,1,MPI_UNSIGNED_LONG,0,1,MPI_COMM_WORLD);
		MPI_Send(c,1,MPI_UNSIGNED_LONG,0,2,MPI_COMM_WORLD);	
        write_log ("sent");
	}

	MPI_Finalize();
    return 0;
	}
