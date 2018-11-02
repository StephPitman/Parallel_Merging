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


int generate_array(char * filename, long size){
      
    FILE *f;
    
    f = fopen(fileName,"w");
    
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
long serial_merge(long *a, long *b, long *c, long a_start, long a_end, long b_start, long b_end){
	long i=a_start;
	long j=b_start;
	long k=a_start+b_start;
	long n=a_end-a_start+b_end-b_start;
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
	size = end - start;
	a=malloc(sizeof(long)*(size));
	fseek(file,start,SEEK_SET);
	fread(a,4,size,file);
	fclose(file);
	
	return size;
	
	
}

long main(long argc, char **argv){
	char *file1;
	char *file2;
	char *file3;
	
	file1=argv[0];
	file2=argv[2];
	file3=argv[4];

	long n1=argv[1];
	long n2=argv[3];
	generate_array(file1, n1);
	generate_array(file2, n2);
	
	MPI_Init(&argc, &argv);
	int id,p;
	long *a;
	long *b;
	long *c;
	long *bigC:
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
		i_0=id*cs;
		i_1=(id+1)*chunk_size;
		size_a=chunk_size;
		load_array(file1, i_0,i_1, a);
		
		j_0=binary_search(file2,n2,i_0);
		j_1=binary_search(file2,n2,i_1);
		
		b_size = load_array(file2, j_0,j_1,b);
		serial_merge(a,b,c,0,chunk_size,0, b_size);
		
	}else{
		i_0=id*cs;
		i_1=n1-1;
		size_a=i_1-i_0;
		load_array(file1, i_0,i_1, a);		
		j_0=binary_search(file2,n2,i_0);
		j_1=n2-1;
		b_size = load_array(file2, j_0,j_1,b);
		serial_merge(a,b,c,0,size_a,0,b_size);
	}
	
	if (id==0){
		bigC=malloc(sizeof(long)*n3);
		long offset = 0;
		for(source=1;source<p;source++){
			long rcv_size;
			MPI_Recv(&rcv_size,1,MPI_UNSIGNED_LONG,source,1,MPI_COMM_WORLD,&status);
			MPI_Recv(bigC+offset,rcv_size,MPI_UNSIGNED_LONG,source,2,MPI_COMM_WORLD,&status);
			offset+=rcv_size;
		}
		FILE *f;
		f = fopen(file3,"w");
		fwrite(bigC,4,offset,f);
		fclose(f);

	}else{
		MPI_Send(&size_a,1,MPI_UNSIGNED_LONG,0,1,MPI_COMM_WORLD);
		MPI_Send(c,1,MPI_UNSIGNED_LONG,0,2,MPI_COMM_WORLD);	
	}

	MPI_Finalize();
	}
	return 0;
}
