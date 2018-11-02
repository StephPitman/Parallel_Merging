/*
 * serial_merging.c
 *
 *  Created on: Nov 1, 2018
 *      Author: Evgeny Zhereshchin
 */

#include <stdio.h>
#include <math.h>
#include <stdbool.h>

/*used wikipedia as reference
 *Inputs:
 *		-array of integers to be searched
 *		-size of the array
 *		-target value to find
 *Output:
 *		-the index of the value or closes value that is smaller than target
*/
int binary_search(int *array, int n, int target){
	int L=0;
	int R=n;
	int m;
	while(L<R){
		m=floor(L+(R-L)/2);
		if (array[m]< target){
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
void serial_merge(int *a, int *b, int *c, int a_start, int a_end, int b_start, int b_end){
	int i=a_start;
	int j=b_start;
	int k=a_start+b_start;
	int n2=a_end-a_start+b_end-b_start;
	while (k<n2){
		int a_i=a[i];
		int b_j=b[j];
		if (a_i<=b_j){
			c[k]=a_i;
			i++;
		}else{
			c[k]=b_j;
			j++;
		}
		k++;
	}
}

int main(int argc, char **argv){
	int n; //size of the input arrays
	int n2=2*n; //size of the output array
	int k = floor(log2(n)); //size of the partition of array A
	int r = floor(n/k); // number of partitions of each array
	int i_0, i_1;  //start and finish of the segment A to merge
	int j_0, j_1; //start and finish of the segment B to merge
	int target;
	int a[n]; //first input array
	int b[n]; //second input array
	int c[n2]; //output array

	for (int i = 0; i<r;i++){
		i_0=a[(i-1)*k];
		i_1=a[i*k];
		j_0=binary_search(b,n,i_0);
		j_1=binary_search(b,n,i_1);
		serial_merge(a,b,c,i_0,i_1,j_0,j_1);
	}
	return 0;
}
