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
