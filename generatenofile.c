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
