#include <stdio.h>
#include <math.h>
#include <stdbool.h>
#include <mpi.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

char buff[100];






void generate_array( long *array, long size){
      
      
    
    
    long y = 0;
    for (long x = 0; x < size; x++){
        int t = rand() % 20;
        
      
       y+= (t+1);
           
       
        
       array[x] = y;
        
        
        sprintf(buff, " generated array[%d]=%d",x,y);
        write_log (buff);
 //               
    }
    
 
}

/*
 *Inputs:
 *		-array of integers to be searched
 *		-size of the array
 *		-target value to find
 *Output:
 *		-the index of the value or closes value that is smaller than target
*/

long binary_search(long *array, long n, long target){
	long L=0;
	long R=n-1;
	long m;
	if (target>=array[n-1]){
		return n-1;
	}
	while(L<R){
		m=floor(L+(R-L)/2);
		long value = array[m];
		if (value > target){
			R=m;
		}else{
			if (L==m){
				R=m;
			}else{
				L=m;
			}


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
void serial_merge(long *a, long *b, long *c, long i_0, long j_0, long i_1, long j_1) {
	long i = i_0;
	long j = j_0;
	long a_size = i_1-i_0+1;
    long b_size = j_1-j_0+1;
    long k = 0;
	long b_j;
	long a_i;
	long c_size = a_size + b_size;
    
    
	while (k < c_size && i <= (i_1) && j <= (j_1)) {
		a_i = a[i];
		b_j = b[j];
		if (a_i <= b_j) {
			c[k] = a_i;
			i++;
		} else {
			c[k] = b_j;
			j++;
		}
		k++;
	}
    
    //printf ("before append id = %d,  i= %d, j=%d, k=%d\n", ID, i,j,k);
	if (i > (i_1)) {
		for (; j <= (j_1); j++) {
			b_j = b[j];
			c[k] = b_j;
			
			k++;
		}
	} else if (j > (j_1)) {
		for (; i <= (i_1); i++) {
			a_i = a[i];
			c[k] = a_i;
			
			k++;
		}
	}
    // printf ("after appending leftover id = %d,  i= %d, j=%d, k=%d\n", ID, i,j,k);

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
	
    srand(time(NULL));
    
    MPI_Init(&argc, &argv);
    char *file1;
	int id,p;
	long *a;
	long *b;
	long *c;
	long *bigC;
    
    
    long n1=atol(argv[2]);
	long n2=atol(argv[3]);
    
	long n3 = n1+n2;
    
    
    
    write_log("started main");
	
    MPI_Status status;
	
    
    file1=argv[1];
	
    a = malloc (sizeof(long)*n1);
    b = malloc (sizeof(long)*n2);
    write_log( " parsed inputs");

	
	
	
	
	
	MPI_Comm_rank (MPI_COMM_WORLD, &id);
	MPI_Comm_size (MPI_COMM_WORLD, &p);
    
	if (id==0){
        
        
    
        
		generate_array(a,n1);
		generate_array(b,n2);
		
		MPI_Bcast(a,n1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(b,n2,MPI_LONG,0,MPI_COMM_WORLD);
        
       
	}
	else{
		write_log ("started bcast rcv");
		MPI_Bcast(a,n1,MPI_LONG,0,MPI_COMM_WORLD);
		MPI_Bcast(b,n2,MPI_LONG,0,MPI_COMM_WORLD);
        
    /*     for (int z=  0; z < n1  ; z++){
            printf ("after bcast <id:%d> a[%d] = %d \n", id,z, a[z]);
            fflush(stdout);
        }
        
         for (int z=  0; z < n2  ; z++){
            printf ("after bcast <id:%d> b[%d] = %d \n", id,z, b[z]);
             fflush(stdout);
        }
        */
        
        
        write_log ("ended bcast rcv");
	}
	long i_0, i_1;  //start and finish of the segment A to merge
	long j_0, j_1; //start and finish of the segment B to merge

	int chunk_size=n1/p;
	long a_size, b_size, c_size;
	//0 cs 1cs
	//1cs+1 2cs
	//2cs+1 3cs
    
    
    
    
	//if (id!=p-1){
        
        if (id == 0 ){
            i_0=0;
            
            
            j_0 = 0;
        }else {
            i_0=id*chunk_size;
            
            
            j_0=binary_search(b,n2,a[i_0-1])+1;
        }
        
		if (id < p-1){
            i_1=(id+1)*chunk_size-1;
            
            
            j_1=binary_search(b,n2,a[i_1]);
        }else{
           
            i_1= n1-1;
            
            j_1=n2-1;
        }
        
        
		
        
        
        write_log("decided on indexes for first array");
        write_log("decided on indexes for 2nd array");
		a_size=i_1 - i_0+1;
        b_size = j_1- j_0+1;
		
        if (j_1 == j_0){
            b_size = j_1- j_0;
        }
        
        c_size = a_size + b_size;
        
        
        
        
        write_log("merging chunk");
       
    /*  printf ("before  merge <id:%d> i0:a[%d]=%d, i1:a[%d]=%d, irange= %d, j0:b[%d]=%d,  j1:b[%d]=%d , jrange=%d, csize=%D\n",
	      id, i_0,a[i_0],i_1, a[i_1],a_size,j_0, b[j_0], j_1, b[j_1],b_size,c_size );
         
        for (int z=  0; z < n1  ; z++){
            printf ("before  merge <id:%d> a[%d] = %d \n", id,z, a[z]);
            
        }
        fflush(stdout);
         for (int z=  0; z < n2  ; z++){
            printf ("before  merge <id:%d> b[%d] = %d \n", id,z, b[z]);
             
        }
        fflush(stdout);*/
        
        c=malloc(sizeof(long)*(c_size));
		serial_merge(a,b,c,i_0, j_0,i_1,j_1);
        /*
        for (int z=  0; z < c_size  ; z++){
            printf ("<id:%d> c[%d] = %d \n", id,z, c[z]);
            
        }*/
        
        write_log("merged chunk");
       
	
	if (id==0){
        write_log ("receiving");
		bigC=malloc(sizeof(long)*n3);
		long offset = 0;
        
        for (int z = 0; z < c_size; z++){
              bigC[z] = c[z];
        }
        offset += c_size;
        
		for(int source=1;source<p;source++){
            write_log ("receiving proc");
            
          
			long rcv_size;
			MPI_Recv(&rcv_size,1,MPI_LONG,source,1,MPI_COMM_WORLD,&status);
			MPI_Recv(bigC+offset,rcv_size,MPI_LONG,source,2,MPI_COMM_WORLD,&status);
			offset+=rcv_size;
		}
        write_log ("received all");
		FILE *f;
		f = fopen(file1,"w");
        
        for (int z = 0; z < offset-1; z++){
            sprintf(buff,"%d,",*(bigC+z));
            fwrite (buff,1,strlen(buff),f);
        }
        sprintf(buff,"%d",*(bigC+offset-1));
        fwrite (buff,1,strlen(buff),f);
        
		//fwrite(bigC,4,offset,f);
		fclose(f);

	}else{
        write_log ("sending"); 
		MPI_Send(&c_size,1,MPI_LONG,0,1,MPI_COMM_WORLD);
		MPI_Send(c,c_size,MPI_LONG,0,2,MPI_COMM_WORLD);	
        write_log ("sent");
	}

	MPI_Finalize();
    return 0;
	}
