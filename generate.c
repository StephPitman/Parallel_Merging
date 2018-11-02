#include <time.h>
#include <stdlib.h>
#include <stdio.h>

int main (int nargs, char *args[]){
    
    
    
    
    char *defaultFileName = "list1";
    
    char *sizeParam = "-size=";
    char *nameParam = "-file=";
    
    int size = 1000000;
    char* fileName = defaultFileName;
    
    

    for (int z = 0; z < nargs; z++){

        char *c = args[z];
    
        char *sp = strstr(   c ,sizeParam );
        char *np = strstr(   c ,nameParam );
            
    
    
        if (sp != NULL && sp[0] == c[0]){
            sp += strlen(sizeParam);
            size = atoi(sp);
            
        }else if (np != NULL && np[0] == c[0]){
            np += strlen(nameParam);
            fileName = np;
        }
    }    
    
    

    printf("generating file:%s size:%d\n", fileName, size);
    
    
    
    
    
    
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
            y++;
            fwrite(&y, sizeof(y), 1, f);
        }
        
 //       printf("%d\n", y);
        

        



    }
    
    fclose(f);
    
    
    
    
    
    return 0;
}
