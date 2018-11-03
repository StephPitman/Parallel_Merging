#include <stdio.h>
#include <math.h>
#include <stdbool.h>
#include <mpi.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>

char buff[100];
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
    
    char ibuff[100];
    
    sprintf(ibuff,"<id:%d>", ID);
    
    fwrite (ibuff, 1, strlen(ibuff),logf);
    
    fwrite (text, 1, strlen(text),logf);
    
    fwrite ("\n", 1, 1,logf);
    
    fclose(logf);
    
}



void generate_array( long *array, long size){
      
      
    
    
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
    
 
}


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
void serial_merge(long *a, long *b, long **c, long i_0, long j_0, long i_1, long j_1) {
	long i = i_0;
	long j = j_0;
	long a_size = i_1-i_0;
    long b_size = j_1-j_0;
    long k = 0;
	long b_j;
	long a_i;
	long n2 = a_size + b_size;
    
    *c=malloc(sizeof(long)*n2);
	while (k < n2 && i < a_size && j < b_size) {
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
		for (; j < b_size; j++) {
			b_j = b[j];
			(*c)[k] = b_j;
			j++;
			k++;
		}
	} else if (j >= b_size) {
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
  	ID = id;
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
	long a_size, b_size;
	//0 cs 1cs
	//1cs+1 2cs
	//2cs+1 3cs
	if (id!=p-1){
		i_0=id*chunk_size;
		i_1=(id+1)*chunk_size;
        
        
            write_log("decided on indexes for first array");
     
		a_size=chunk_size;
		j_0=binary_search(b,n2,a[i_0]);
		j_1=binary_search(b,n2,a[i_1]);
		
        write_log("decided on indexes for 2nd array");
        
        
		b_size=j_1-j_0;
        
        write_log("merging chunk");
        
        printf ("before  merge <id:%d> i0:%d,j0:%d,i1:%d,j1:%d",id, i_0,j_0, i_1, j_1);
        
        
        for (int z=  0; z < n1  ; z++){
            printf ("before  merge <id:%d> a[%d] = %d \n", id,z, a[z]);
            fflush(stdout);
        }
        
         for (int z=  0; z < n2  ; z++){
            printf ("before  merge <id:%d> b[%d] = %d \n", id,z, b[z]);
             fflush(stdout);
        }
        
        
		serial_merge(a,b,&c,i_0, j_0,i_1,j_1);
        
        for (int z=  0; z < a_size + j_1 -j_0 ; z++){
            printf ("<id:%d> c[%d] = %d \n", id,z, c[z]);
            
        }
        
        write_log("merged chunk");
        
	}else{
		i_0=id*chunk_size;
		i_1=n1-1;
		a_size=i_1-i_0;
        
        write_log("decided on indexes for first array");
       
        
		j_0=binary_search(b,n2,a[i_0]);
		j_1=n2-1;
        
        write_log("decided on indexes for 2nd array");
        write_log("merging chunk");
		serial_merge(a,b,&c,i_0, j_0,i_1,j_1);
        
        
        
        
        for (int z=  0; z < a_size + j_1 -j_0 ; z++){
            printf ("<id:%d> c[%d] = %d \n", id,z, c[z]);
            
        }
        
        
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
        
        for (int z = 0; z < offset-1; z++){
            sprintf(buff,"%d,",*(bigC+z));
            fwrite (buff,1,strlen(buff),f);
        }
        sprintf(buff,"%d,",*(bigC+offset-1));
        fwrite (buff,1,strlen(buff),f);
        
		//fwrite(bigC,4,offset,f);
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
