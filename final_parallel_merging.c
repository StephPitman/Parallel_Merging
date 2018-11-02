/*
 * serial_merging.c
 *
 *  Created on: Nov 1, 2018
 *      Author: Evgeny Zhereshchin
 */

#include <stdio.h>
#include <math.h>
#include <stdbool.h>
#include <mpi.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>


int generate_array(char * filename, long size){
      
    FILE *f;
    
    f = fopen(filename,"w");
    
    if (f==NULL){
        printf("file can't be opened for writing\n");
        return -1;
    }
    if (size <1){
        printf("invalid size\n");
        return -1;
    }
        
    srand(time(NULL));   // Initialization, should only be called once.
        
    long y = 0;
    for (long x = 0; x < size; x++){
        int t = rand() % 3;
        
        if (t == 0){
			t = rand() % 3;
            y+=t+1;
            fwrite(&y, sizeof(y), 1, f);
        }
 //       printf("%d\n", y);        
    }
    
    fclose(f);

    return 0;
}


/*used wikipedia as reference
 *Inputs:
 *		-array of integers to be searched
 *		-size of the array
 *		-target value to find
 *Output:
 *		-the index of the value or closes value that is smaller than target
*/
long binary_search(FILE *file, long n, long target){
	long L=0;
	long R=n;
	long m;
	while(L<R){
		m=floor(L+(R-L)/2);
		fseek(file,m*4,SEEK_SET);
		long value = 0;
		fread(&value,4,1,file);
		if (value < target){
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
long serial_merge(long *a, long *b, long *c, long sizea, long sizeb){
	long i=0;
	long j=0;
	long k=0;
	long n=sizea+sizeb;
	c=malloc(sizeof(long)*n);
	while (k<n){
		long a_i=a[i];
		long b_j=b[j];
		if (a_i<=b_j){
			c[k]=a_i;
			i++;
		}else{
			c[k]=b_j;
			j++;
		}
		k++;
	}
	
	return n;
}

long load_array(FILE *file, long start, long end, long *a){
	long size = end - start;
	a=malloc(sizeof(long)*(size));
	fseek(file,start,SEEK_SET);
	fread(a,4,size,file);
	fclose(file);
	
	return size;
	
	
}


void write_log (char *text ){
    FILE *logf;
    logf = fopen ("log1.txt", "a");
    
    fwrite (text, 1, strlen(text),logf);
    
    fwrite ("\n", 1, 1,logf);
    
    fclose(logf);
    
}

long main(int argc, char **argv){
	char *file1;
	char *file2;
	char *file3;
    
    
    
    
     write_log("started main");
	
    MPI_Status status;
	
    
   
    
    file1=argv[1];
	file2=argv[3];
	file3=argv[5];
    
    
  

	long n1=atol(argv[2]);
	long n2=atol(argv[4]);
    
    write_log( " parsed inputs");
     
    
	
    generate_array(file1, n1);
	 write_log("generated 1");
    
    
    generate_array(file2, n2);
      write_log("generated 2");
    
    
    
	
	MPI_Init(&argc, &argv);
	int id,p;
	long *a;
	long *b;
	long *c;
	long *bigC;
	long n3 = n1+n2;
	FILE  *f1 =NULL;
	FILE  *f2 =NULL;
	
	f1 = fopen(file1,"r");
	f2 = fopen(file2,"r");
	
	MPI_Comm_rank (MPI_COMM_WORLD, &id);
  MPI_Comm_size (MPI_COMM_WORLD, &p);
	
	long i_0, i_1;  //start and finish of the segment A to merge
	long j_0, j_1; //start and finish of the segment B to merge

	int chunk_size=n1/p;
	long b_size;
	long size_a;
	//0 cs 1cs
	//1cs+1 2cs
	//2cs+1 3cs
	if (id!=p-1){
		i_0=id*chunk_size;
		i_1=(id+1)*chunk_size;
        
        
            write_log("decided on indexes for first array");
     
		size_a=chunk_size;
		load_array(f1, i_0,i_1, a);
		
		j_0=binary_search(f2,n2,i_0);
		j_1=binary_search(f2,n2,i_1);
		
        write_log("decided on indexes for 2nd array");
        
        
		b_size = load_array(f2, j_0,j_1,b);
        
        write_log("merging chunk");
		serial_merge(a,b,c,chunk_size, b_size);
        write_log("merged chunk");
        
	}else{
		i_0=id*chunk_size;
		i_1=n1-1;
		size_a=i_1-i_0;
        
        write_log("decided on indexes for first array");
       
        

		load_array(f1, i_0,i_1, a);		
		j_0=binary_search(f2,n2,i_0);
		j_1=n2-1;
        
        write_log("decided on indexes for 2nd array");
		b_size = load_array(f2, j_0,j_1,b);
        write_log("merging chunk");
		serial_merge(a,b,c,size_a,b_size);
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
		f = fopen(file3,"w");
		fwrite(bigC,4,offset,f);
		fclose(f);

	}else{
        write_log ("sending"); 
		MPI_Send(&size_a,1,MPI_UNSIGNED_LONG,0,1,MPI_COMM_WORLD);
		MPI_Send(c,1,MPI_UNSIGNED_LONG,0,2,MPI_COMM_WORLD);	
        write_log ("sent");
	}

	MPI_Finalize();
    return 0;
	}
	

