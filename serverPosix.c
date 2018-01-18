#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>       
#include <arpa/inet.h>      
#include <unistd.h>        
#include <string.h>    
#include <strings.h>  
#include <mqueue.h>
#include <signal.h>
#include <sys/stat.h>
#include <ctype.h>

#define BUFF_SIZE 10000


/* Estructura que tiene la forma de un archivo */
typedef struct fileStruct {
    char name[256];
    char cont[500];
} data;

typedef struct openedFileStruct {
    int idCliente;
    char name[256];    
    int idArchivoWorker;
    int cursor;
} openedFile;

int Errores;
int IDc;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock2 = PTHREAD_MUTEX_INITIALIZER;

struct mq_attr mq_buzon;
mqd_t mqwk1,mqwk2,mqwk3,mqwk4,mqwk5;

void delete(data *Buffer, int i,int *tam)
{
	Buffer[i] = Buffer[*tam - 1];
	memset(&Buffer[*tam - 1], 0, sizeof(data));
	//printf("delete Buffer name %s, tam %d \n",Buffer[*tam - 1].name, *tam);
	*tam = *tam - 1;
}

int tokens(char *s1,char **t2,char *s) 
{
	char *t1;
	int i=0;

    pthread_mutex_lock(&lock);
    
	for(t1 = strtok(s1,s); t1 != NULL; t1 = strtok(NULL, " "))
	{
		t2[i] = t1;
		//printf("puntero: %p\n",t2[i]);
		i++;
		//printf("Lo que va guardando: %s\n",t2[i-1]);
	}
	
	pthread_mutex_unlock(&lock);
	
	return i;
}

void *worker(void *arg)
{
	data Buffers[100];
	openedFile Abiertos[300];
	char *Buffers_recibidos[300];
	char mq_name_array[5][10]; //para guardar los nombres de las colas
	long aux = (long)arg;
	int IDw = aux;	//ID de cada worker
	int conn_s; //socket
	char msgbuf[BUFF_SIZE]; //array donde guardamos el mensaje recibido de la cola
	char msgresp[BUFF_SIZE]; //array donde guardamos el mensaje que enviamos en la cola
	unsigned int msgprio;
	mqd_t mqwk,mqwks[6];
	size_t i = 0;
	char* recv[100];
	char* recv2[100];
	int cant = 0, NFD = 0, bandOPN1, bandOPN2, auxx = 0, size = 0;
	int p = 0, j = 0, band1 = 0, band2 = 0, band3 = 0, bandDEL1 = 0, bandDEL2 = 0, b = 0, cant_recv = 0, tam_buf = 0, bandCRE = 0, bandCRE1 = 0, bandCRE2 = 0, bandCRE3 = 0; 
	char buf[100], argAUX[100];

	//printf("Trabajador %d \n",IDw);
	
	
	// Reservamos memoria:
	for (j = 0; j < 100; j++) 
		Buffers_recibidos[j] = malloc(100);

	// Inicializamos los arrays en 0
	memset(&msgbuf, 0,BUFF_SIZE*sizeof(char));
	memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
	memset(&buf, 0,100*sizeof(char));
	
	// Abro la cola de cada uno de los workers
	for (j = 1; j <= 5; j++)
	{	
		sprintf(mq_name_array[j], "/cola%d", j);
		mqwks[j] = mq_open(mq_name_array[j], O_RDWR);
		if(mqwks[j] < 0)
			perror("mq_open:worker");
		
	}
	
	mqwk = mqwks[IDw]; //mqwk es la cola de este worker
	
	while(i != -1)
	{
		//inicializo los arrays en 0
		memset(&msgbuf, 0,BUFF_SIZE*sizeof(char));
	    memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
	    memset(&recv, 0,100*sizeof(char*));
	    memset(&recv2, 0,100*sizeof(char*));
	    memset(&argAUX, 0,100*sizeof(char));
		band1 = 0;
		auxx = 0;

	    
	    // Entramos en el receive. Retorna el n° de bytes recibidos. -1 en caso de error. Empezamos a recibir mensajes.
	    
	    /* mq_receive() removes the oldest message with the highest priority from the message queue referred to by 
	     * the descriptor mqdes, and places it in the buffer pointed to by msg_ptr. The msg_len argument specifies 
	     * the size of the buffer pointed to by msg_ptr; this must be greater than the mq_msgsize attribute of the 
	     * queue (see mq_getattr(3)). If prio is not NULL, then the buffer to which it points is used to return the 
	     * priority associated with the received message. If the queue is empty, then, by default, mq_receive() 
	     * blocks until a message becomes available, or the call is interrupted by a signal handler. If the O_NONBLOCK 
	     * flag is enabled for the message queue description, then the call instead fails immediately with the error EAGAIN. */
	     
	    /* Va a quitar el msj más viejo con más alta prioridad y colocar lo que tiene msgbuf */
	    /* Le estamos mandando el socket del cliente */
	   
		//for(j = 0; j < NFD; j++)
			//printf("Abiertos = %s\n", Abiertos[j].name);
 
		i = mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);	
		if (i == -1) // En caso de error de mq_receive:
		{
			perror("mq_receive():worker");
			exit(1);
		}
		else
		{
			cant = tokens(msgbuf, recv, " ");

			/*-------------------------------- LSD ---------------------------------*/ 	
			if (strcmp(recv[0], "LSDPedido") == 0)// Mandamos una señal a los demás workers para que nos manden la info necesaria para el LS:	
			{

				sprintf(msgresp, "%d", tam_buf); 
				
				// Mando el tamaño del buf (tam_buf) al worker que tiene el socket del cliente
 				if(mq_send(mqwks[atoi(recv[1])], msgresp, strlen(msgresp), 3) < 0)
 						perror("mq_send3:worker");
				
				// Mandamos los nombres de los archivos con prioridad 2
				for(j = 0; j < tam_buf; j++)
				{	
					sprintf(msgresp, "%s", Buffers[j].name); 
 					if(mq_send(mqwks[atoi(recv[1])], msgresp, strlen(msgresp), 2) < 0)
 						perror("mq_send2:worker");
 					memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
				}
			}	
			if (strcmp(recv[0],"LSD") == 0) // Caso en el cliente haya mandado "LSD" (el msj se guarda en recv[0] y el socket del cliente en recv[1]
			{
				conn_s = atoi(recv[1]);
				//printf("FUNCION LSD - SOCKET %d\n", conn_s);
				
				// "LSDPedido por el worker IDw"
				sprintf(msgresp, "LSDPedido %d", IDw);
				
				// Mandamos "LSDPedido IDw" a todos los demás workers menos al actual:
				for(j = 1; j <= 5; j++)
				{
					if(j != IDw)
					{
						if(mq_send(mqwks[j], msgresp, strlen(msgresp), 1) < 0)
							perror("mq_send1: worker");
					}
				}
				
				while(1)
				{
					i = 0;
					
					memset(&msgbuf, 0, BUFF_SIZE*sizeof(char));
					memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
					memset(&recv2, 0, 100*sizeof(char*));
					
					i = mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);
					
					if (i == -1) // En caso de error de mq_receive:
					{
						perror("mq_receive():worker");
						exit(1);
					}
					
					cant = tokens(msgbuf, recv2, " ");
		
					if(msgprio == 3)
					{
						cant_recv = cant_recv + atoi(recv2[0]);
						band3++;
					
						if(band3 == 4 && cant_recv == 0)
						{
							strcat(msgresp,"OK ");
							for(j=0; j<tam_buf;j++)
							{
								
								strcat(msgresp,Buffers[j].name);
								strcat(msgresp," "); 
							}
							
							strcat(msgresp,"\n");

							if(write(conn_s, msgresp, strlen(msgresp))<0)
								perror("write:worker");
							band2 = 0;
							band3 = 0;
							cant_recv = 0;
							break;
						}
					}	
					if ((band3 != 4 && msgprio != 3) || (msgprio != 2 && msgprio != 3))
					{
						
						if(mq_send(mqwk,msgbuf,strlen(msgbuf),msgprio)<0) //Devuelvo el mensaje a la cola para no perderlo
							perror("mq_send: worker");
					}
					if (msgprio == 2 && band3 == 4)
					{
						
						strcpy(Buffers_recibidos[band2], recv2[0]);// Copia el recv2[0] en Buffers_recibidos[a]:
						
						if (band2 == (cant_recv - 1))// Quiere decir que llegó todo el contendido de todos los Workers
						{
	
							strcat(msgresp,"OK ");
							
							for (b = 0; b < (band2 + 1); b++)
							{
								strcat(msgresp, Buffers_recibidos[b]);
								strcat(msgresp, " ");
								if (b == band2)
									for (j = 0; j < tam_buf; j++)
									{
										
										strcat(msgresp, Buffers[j].name); 
										strcat(msgresp, " "); 
									}
							}
								
							strcat(msgresp, "\n");

							if (write(conn_s, msgresp, strlen(msgresp)) < 0)
								perror("write:worker");
							band2 = 0;
							band3 = 0;
							cant_recv = 0;
							break;
						}
						else
						  band2++;
						  
					}
					
				}
			}		
			/*------------------------------------ DEL ------------------------------------*/
			if (strcmp(recv[0], "DELPedido") == 0)// Mandamos una señal a los demás workers para que nos manden la info necesaria para el DEL
			{
				for (j = 0; j < tam_buf; j++)
				{
	
					if (strcmp(Buffers[j].name, recv[1]) == 0)
					{
						delete(Buffers, j, &tam_buf);
					
						sprintf(msgresp, "OK");
						if (mq_send(mqwks[atoi(recv[2])], msgresp, 2, 4) < 0)
							perror("mq_send2:worker");
						band1 = 1;
					}
				}
				if (band1 == 0)
				{
					sprintf(msgresp, "EBADARG");
					if (mq_send(mqwks[atoi(recv[2])], msgresp, 7, 4) < 0)
						perror("mq_send2:worker");
				}	
			}			
			if (strcmp(recv[0], "ABIERTOSPedido") == 0)
			{
				
				
				for (j = 0; j < NFD; j++)
					if(strcmp(Abiertos[j].name, recv[1]) == 0)
					{
						sprintf(msgresp, "EOPNFILE");
						if (mq_send(mqwks[atoi(recv[2])], msgresp, strlen(msgresp), 5) < 0)
							perror("mq_send2:worker");
						band1 = 1;
					}
				
				if (band1 == 0)
				{
					sprintf(msgresp, "OK");
					if(mq_send(mqwks[atoi(recv[2])], msgresp, strlen(msgresp), 5) < 0)
						perror("mq_send2:worker");
				}		
								
			}		
			if (strcmp(recv[0], "DEL") == 0)
			{
				memset(&argAUX, 0, 100*sizeof(char));
				//printf("FUNCION DEL\n");
				conn_s = atoi(recv[2]);
				strcpy(argAUX, recv[1]);
				
				for (j = 0; j < NFD; j++)
					if (strcmp(Abiertos[j].name, argAUX) == 0)
							band1 = 1;
					

					
				if (band1 == 0)
				{
					sprintf(msgresp, "ABIERTOSPedido %s %d", argAUX, IDw);
					for (j = 1; j <= 5; j++)
					{
						if (j != IDw)
						{
							if (mq_send(mqwks[j], msgresp, strlen(msgresp), 1) < 0)
								perror("mq_send1: worker");
						}
					}
			
					
					while(1)
					{
						memset(&msgbuf, 0,BUFF_SIZE*sizeof(char));
						memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
						memset(&recv2, 0,100*sizeof(char*));
						
						// Recibimos datos del worker actual
						mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);
						
						cant = tokens(msgbuf, recv2, " ");
				
						
						if (msgprio == 5)
						{
							if (strcmp(recv2[0], "OK") == 0)
								bandDEL1++;
							if (strcmp(recv2[0], "EOPNFILE") == 0 )
							{
								bandDEL2++;
								bandDEL1++;			
							}
							
							if (bandDEL1 == 4 && bandDEL2 == 0) 
							{	
								bandDEL1 = 0;
								bandDEL2 = 0;
								break;
							}
							if (bandDEL1 == 4 && bandDEL2 > 0) 
							{
								band1 = 1;
								bandDEL1 = 0;
								bandDEL2 = 0;	
								break;
							}
							
	
						}
						else
						{
							//printf("meti de nuevo el msj en la cola DEL\n");
							if (mq_send(mqwk, msgbuf, strlen(msgbuf), msgprio) < 0) //Devuelvo el mensaje a la cola para no perderlo
								perror("mq_send: worker");
						}		
					}
				}
					
	
				if (band1 == 0)
				{
					memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
					for (j = 0; j < tam_buf; j++)
					{

						if (strcmp(Buffers[j].name, argAUX) == 0)
						{
							delete(Buffers, j, &tam_buf);
							
							// Escribo que la operación terminó bien.
							sprintf(msgresp, "OK\n");
							if (write(conn_s, msgresp, 3) < 0)
								perror("write:worker");
							
							band1 = 2;
						}
					}
					if (band1 == 0)
					{
						memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
						// Mandamos "DELPedido (socket del cliente) (worker)":
						sprintf(msgresp, "DELPedido %s %d", argAUX, IDw);
						
						// Le mandamos lo anteriror a todos los workers menos al mismo; con prioridad 1:
						for(j = 1; j <= 5; j++)
						{
							if (j != IDw)
							{
								if (mq_send(mqwks[j], msgresp, strlen(msgresp), 1) < 0)
									perror("mq_send1: worker");
							}
						}
						
						while (1)
						{
							memset(&msgbuf, 0, BUFF_SIZE*sizeof(char));
							memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
							memset(&recv2, 0, 100*sizeof(char*));
							
							// Recibimos datos del worker actual
							mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);
							
							cant = tokens(msgbuf, recv2, " ");
							
							if (msgprio == 4)
							{	

								if (strcmp(recv2[0], "EBADARG") == 0)
									bandDEL1++;
								if (strcmp(recv2[0], "OK") == 0 )
								{
									bandDEL1++;
									bandDEL2++;
								}
								if (bandDEL1 == 4 && bandDEL2 == 0) 
								{
									sprintf(msgresp,"ERROR %d EBADARG\n",Errores);
									pthread_mutex_lock(&lock2);
									Errores++;
									pthread_mutex_unlock(&lock2);	
									if(write(conn_s,msgresp, strlen(msgresp)) < 0)
										perror("write:worker");
									bandDEL1=0;
									bandDEL2=0;
									break;
								}
								if (bandDEL1 == 4 && bandDEL2 > 0) 
								{	
									if(write(conn_s, "OK\n", strlen("OK\n"))<0)
										perror("write:worker");
									bandDEL1 = 0;
									bandDEL2 = 0;
									break;
								}
							}
							if (msgprio != 4)
							{
								//printf("meti de nuevo el msj en la cola DEL\n");
								if (mq_send(mqwk, msgbuf, strlen(msgbuf), msgprio) < 0) //Devuelvo el mensaje a la cola para no perderlo
									perror("mq_send: worker");
							}	
						}
					}
				}
			
				if (band1 == 1)
				{
					memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
					sprintf(msgresp, "ERROR %d EOPNFILE\n",Errores);
					pthread_mutex_lock(&lock2);
					Errores++;
					pthread_mutex_unlock(&lock2);
					if (write(conn_s, msgresp, strlen(msgresp)) < 0)
								perror("write:worker");
				}
					
			}			
		    /*----------------------------- CRE ----------------------------------*/
			if (strcmp(recv[0],"CREPedido") == 0) // Mandamos una señal a los demás workers para que nos manden la info necesaria para el CRE
			{
				for (j = 0; j < tam_buf; j++)
				{

					if (strcmp(Buffers[j].name, recv[1]) == 0)
					{
					bandCRE1 = 1;
					sprintf(msgresp, "SI %s", recv[1]);
					if (mq_send(mqwks[atoi(recv[2])], msgresp, 3 + strlen(recv[1]), 6) < 0)
						perror("mq_send2:worker");			
					}
				}
				if (bandCRE1 == 0)
				{
					sprintf(msgresp, "NO %s", recv[1]);
					if(mq_send(mqwks[atoi(recv[2])], msgresp, 3 + strlen(recv[1]), 6) < 0)
						perror("mq_send2:worker");
				}	
			}					
			if (strcmp(recv[0], "CRE") == 0)
			{
				//printf("FUNCION CRE\n");	
				conn_s = atoi(recv[cant - 1]);
					
				for (j = 0; j < tam_buf; j++)
				{

					if (strcmp(Buffers[j].name, recv[1]) == 0)
					{
						sprintf(msgresp, "ERROR %d EBADARG\n",Errores);
						pthread_mutex_lock(&lock2);
						Errores++;
						pthread_mutex_unlock(&lock2);
						if (write(conn_s, msgresp, strlen(msgresp)) < 0)
							perror("write:worker");
						bandCRE = 1;	
					}
				}	
				if (bandCRE == 0)
				{
					
					sprintf(msgresp, "CREPedido %s %d", recv[1], IDw);
					
					for (j = 1; j <= 5; j++)
					{
						if (j != IDw)
						{
							if (mq_send(mqwks[j], msgresp, strlen(msgresp), 1) < 0)
								perror("mq_send1: worker");
						}
					}	
					
					while (1)
					{
						i = 0;
						memset(&msgbuf, 0, BUFF_SIZE*sizeof(char));
						memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
						memset(&recv2, 0, 100*sizeof(char*));
						i = mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);
						if (i == -1) // En caso de error de mq_receive:
						{
							perror("mq_receive():workerAUX");
							exit(1);
						}
						cant = tokens(msgbuf,recv2," ");

	
							
							if (msgprio == 6)
							{	

								if (strcmp(recv2[0], "NO") == 0)
								{
									bandCRE2++;

								}
								if (strcmp(recv2[0], "SI") == 0)
								{
									bandCRE2++;
									bandCRE3++;

								}							
									
								if (bandCRE2 == 4 && bandCRE3 == 0)
								{
									sprintf(buf, "%s", recv2[1]);
									strcpy(Buffers[tam_buf].name, buf); 
									tam_buf++;
									sprintf(msgresp, "OK\n");
									if (write(conn_s, msgresp, 3) < 0)
										perror("write:worker");
									bandCRE = 0;
						            bandCRE1 = 0;
									bandCRE2 = 0;
									bandCRE3 = 0;		
									break;
								}
								if (bandCRE3 > 0 && bandCRE2 == 4)
								{
									sprintf(msgresp, "ERROR %d EOPNFILE\n",Errores);
									pthread_mutex_lock(&lock2);
									Errores++;
									pthread_mutex_unlock(&lock2);
									if(write(conn_s, msgresp,strlen(msgresp)) < 0)
										perror("write:worker");
									bandCRE = 0;
						            bandCRE1 = 0;
									bandCRE2 = 0;
									bandCRE3=0;			
									break;
								}	
							}
							if (msgprio != 6)
							{
								//printf("meti de nuevo el msj en la cola CRE\n");
								if(mq_send(mqwk, msgbuf, strlen(msgbuf), msgprio) < 0) //Devuelvo el mensaje a la cola para no perderlo
									perror("mq_send: worker");
							}	
					}
				}
											
					
			}						
			/*---------------------------- OPN --------------------------------*/	
			if (strcmp(recv[0], "OPNPedido") == 0)// Mandamos una señal a los demás workers para que nos manden la info necesaria para el OPN
			{
				for (j = 0; j < tam_buf; j++)
				{
					if (strcmp(Buffers[j].name,recv[1]) == 0)
					{
						sprintf(msgresp,"OK %d\n", IDw);
						
						if (mq_send(mqwks[atoi(recv[2])], msgresp, strlen(msgresp), 7) < 0)
							perror("mq_send2:worker");
						
						band1 = 1;
					}
				}
				if (band1 == 0)
				{
					sprintf(msgresp, "EBADARG");
					if (mq_send(mqwks[atoi(recv[2])], msgresp, strlen(msgresp), 7) < 0)
						perror("mq_send2:worker");
				}	
			}			
			if (strcmp(recv[0], "OPN") == 0)
			{
				//printf("FUNCION OPN\n");
				conn_s = atoi(recv[cant - 1]);
				strcpy(argAUX, recv[1]);
				
				if (cant - 1 != 2)
				{
					sprintf(msgresp, "ERROR %d EBADARG\n",Errores);
					pthread_mutex_lock(&lock2);
					Errores++;
					pthread_mutex_unlock(&lock2);
					if (write(conn_s, msgresp, strlen(msgresp)) < 0)
							perror("write:worker");	
				}
				else
				{
					for(j = 0; j < tam_buf; j++)
					{
						if(strcmp(Buffers[j].name, argAUX) == 0)
						{						

							Abiertos[NFD].idArchivoWorker = IDw;
							strcpy(Abiertos[NFD].name, argAUX);
							Abiertos[NFD].idCliente = conn_s;
							Abiertos[NFD].cursor = 0;
							
							sprintf(msgresp, "OK FD %d\n", NFD);
							if(write(conn_s, msgresp, strlen(msgresp)) < 0)
								perror("write:worker");
					
							NFD++;
							band1 = 1;
						}
					}
								
					if (band1 == 0)
					{
						// Mandamos "OPNPedido (socket del cliente) (worker)":
						sprintf(msgresp, "OPNPedido %s %d", argAUX, IDw);
						
						// Le mandamos lo anteriror a todos los workers menos al mismo; con prioridad 1:
						for(j = 1; j <= 5; j++)
						{
							if(j != IDw)
							{
								if(mq_send(mqwks[j], msgresp, strlen(msgresp), 1) < 0)
									perror("mq_send1: worker");
							}
						}
						
						while (1)
						{
							memset(&msgbuf, 0, BUFF_SIZE*sizeof(char));
							memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
							memset(&recv2, 0, 100*sizeof(char*));
							
							// Recibimos datos del worker actual
							mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);
							
							cant = tokens(msgbuf, recv2, " ");

							
							if(msgprio == 7)
							{		

								if(strcmp(recv2[0], "EBADARG") == 0)
									bandOPN1++;
								if(strcmp(recv2[0], "OK") == 0 )
								{
									bandOPN2++;
									bandOPN1++;
									auxx = atoi(recv2[1]);
									
								}
								if (bandOPN1 == 4 && bandOPN2 == 0) {
									sprintf(msgresp, "ERROR %d EBADARG\n",Errores);
									pthread_mutex_lock(&lock2);
									Errores++;
									pthread_mutex_unlock(&lock2);
									if(write(conn_s, msgresp, strlen(msgresp)) < 0)
										perror("write:worker");
									
									bandOPN1 = 0;
									bandOPN2 = 0;
									
									break;
								}
								if (bandOPN1 == 4 && bandOPN2 > 0) 
								{						
									Abiertos[NFD].idArchivoWorker = auxx;
									strcpy(Abiertos[NFD].name, argAUX);
									Abiertos[NFD].idCliente = conn_s;
									Abiertos[NFD].cursor = 0;


									sprintf(msgresp, "OK FD %d\n", NFD);
									if(write(conn_s, msgresp, strlen(msgresp)) < 0)
										perror("write:worker");
										
									NFD++;
									
									bandOPN1 = 0;
									bandOPN2 = 0;
									
									break;
									
								}
								
									
							}
							
							if (msgprio != 7)
							{
								//printf("meti de nuevo el msj en la cola OPN\n");
								if(mq_send(mqwk, msgbuf, strlen(msgbuf), msgprio) < 0) //Devuelvo el mensaje a la cola para no perderlo
									perror("mq_send: worker");
							}	
						}
					}
				}
			}		
			/*---------------------------- WRT --------------------------------*/
			if (strcmp(recv[0], "WRTPedido") == 0)
			{
				for (j = 0; j < tam_buf; j++) 
				{
					if (strcmp(recv[1], Buffers[j].name) == 0) 
					{
						for (p = 3; p < cant; p++) 
						{
							strcat(argAUX, recv[p]);
							
							if (p != cant - 1)
								strcat(argAUX, " ");

						}
						
						strcat(Buffers[j].cont, argAUX);

						sprintf(msgresp, "OK\n");
						if(write(atoi(recv[2]), msgresp, strlen(msgresp)) < 0)
							perror("write:worker");
					}
				}
			}			
			if ((strcmp(recv[0], "WRT") == 0) && (strcmp(recv[1], "FD") == 0) && (strcmp(recv[3], "SIZE") == 0) )
			{
				//printf("FUNCION WRT\n");
				conn_s = atoi(recv[cant - 1]);
				auxx = atoi(recv[2]);
				
				
				if ((auxx == 0 && (strcmp(recv[2], "0") != 0)) || (atoi(recv[4]) == 0 && (strcmp(recv[4], "0") != 0)) || (cant - 1 <= 4) )
				{
					sprintf(msgresp, "ERROR %d EBADARG\n",Errores);
					pthread_mutex_lock(&lock2);
					Errores++;
					pthread_mutex_unlock(&lock2);
					if(write(conn_s, msgresp, strlen(msgresp)) < 0)
							perror("write:worker");	
				}
				else
				{				
					if (Abiertos[auxx].idCliente == conn_s) 
					{
						band1 = 1;
						band3 = Abiertos[auxx].idArchivoWorker;
					}
					
					if (band1 == 0)
					{
						sprintf(msgresp, "ERROR %d EOPNFILE\n", Errores);
						pthread_mutex_lock(&lock2);
						Errores++;
						pthread_mutex_unlock(&lock2);
						if(write(conn_s, msgresp, strlen(msgresp)) < 0)
								perror("write:worker");	
					}
					else
					{
						memset(&argAUX, 0,100*sizeof(char));
						memset(&buf, 0,100*sizeof(char));

					   
						for (j = 5; j < cant-1; j++) //ya que en cant -1 esta el conn_s
						{
							strcat(argAUX, recv[j]);
							if (j != cant - 2)
								strcat(argAUX, " ");
		
						}
						
						// Size
						size = atoi(recv[4]);

						if (strlen(argAUX) > size + 1)
						{ 
							strncpy(buf, argAUX, size);
							strcpy(argAUX,buf);
						}
											
							
						if (band3 == IDw) 
						{
							for (j = 0; j < tam_buf; j++) 
								if (strcmp(Abiertos[auxx].name, Buffers[j].name) == 0) 
								{
									strcat(Buffers[j].cont, argAUX);
									memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
									sprintf(msgresp, "OK\n");
									if (write(conn_s, msgresp, strlen(msgresp)) < 0)
										perror("write:worker");
									
									band3 = 0;
								} 	
						}
						else 
						{
							memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
							sprintf(msgresp, "WRTPedido %s %d %s", Abiertos[auxx].name, conn_s, argAUX);
							if(mq_send(mqwks[band3], msgresp, strlen(msgresp), 1) < 0)
								perror("mq_send2:worker");
						}
					}
					
				}
				band1 = 0;
				band3 = 0;
			}		
			/*------------------------------ REA --------------------------------------*/
			if (strcmp(recv[0], "REAPedido") == 0)
			{
				memset(&argAUX, 0, 100*sizeof(char));
				
				for (j = 0; j < tam_buf; j++) 
				{	
						if (strcmp(recv[1], Buffers[j].name) == 0) 
						{
							if (atoi(recv[2]) + atoi(recv[3]) <= strlen(Buffers[j].cont))
							{
								strncpy(argAUX,(Buffers[j].cont + atoi(recv[2])), (size_t) atoi(recv[3]));
						
								sprintf(msgresp, "OK SIZE %d %s\n", atoi(recv[3]),argAUX);
								if (mq_send(mqwks[atoi(recv[4])], msgresp, strlen(msgresp), 6) < 0)
									perror("mq_send2:worker");
							}
							else
							{	
								sprintf(msgresp, "OK SIZE 0\n");
								if (mq_send(mqwks[atoi(recv[4])], msgresp, strlen(msgresp), 6) < 0)
									perror("mq_send2:worker");
							}		
						} 
						
					}
			}	
			if ((strcmp(recv[0], "REA") == 0) && (strcmp(recv[1], "FD") == 0) && (strcmp(recv[3], "SIZE") == 0) )
			{
				//printf("FUNCION REA\n");
				conn_s = atoi(recv[cant - 1]);
				auxx = atoi(recv[2]);//NFD
				
				if((auxx == 0 && (strcmp(recv[2], "0") != 0)) || (atoi(recv[4]) == 0 && (strcmp(recv[4], "0") != 0)) || (cant - 1 <= 4) )
				{
					sprintf(msgresp, "ERROR %d EBADARG\n",Errores);
					pthread_mutex_lock(&lock2);
					Errores++;
					pthread_mutex_unlock(&lock2);
					if(write(conn_s, msgresp, strlen(msgresp)) < 0)
							perror("write:worker");	
				}
				else
				{
					size = atoi(recv[4]);
					
					if (Abiertos[auxx].idCliente == conn_s) 
					{
						band1 = 1;
						band3 = Abiertos[auxx].idArchivoWorker;
					}
					
					if (band1 == 0)
					{
						sprintf(msgresp, "ERROR %d EOPNFILE\n",Errores);
						pthread_mutex_lock(&lock2);
						Errores++;
						pthread_mutex_unlock(&lock2);
						if(write(conn_s, msgresp, strlen(msgresp)) < 0)
								perror("read:worker");	
					}
					else
					{
						memset(&argAUX, 0, 100*sizeof(char));

						if (band3 == IDw) 
						{
							for (j = 0; j < tam_buf; j++) 
								if (strcmp(Abiertos[atoi(recv[2])].name, Buffers[j].name) == 0) 
								{
									if (Abiertos[auxx].cursor + size <= strlen(Buffers[j].cont))
									{
										strncpy(argAUX, (Buffers[j].cont + Abiertos[auxx].cursor), size);
										memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
										if(write(conn_s, argAUX, strlen(msgresp)) < 0)
											perror("write:worker");
										Abiertos[auxx].cursor = Abiertos[auxx].cursor + size;
										sprintf(msgresp, "OK SIZE %d %s\n", size, argAUX);
										if (write(conn_s, msgresp, strlen(msgresp)) < 0)
											perror("REA:worker");
										band3 = 0;
									}
									else
									{
										//Abiertos[auxx].cursor = 0;
										if(write(conn_s, "OK SIZE 0\n", 11) < 0)
											perror("REA:worker");
									}		
								} 	
						}
						else 
						{
							memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
							sprintf(msgresp,"REAPedido %s %d %d %d", Abiertos[auxx].name, Abiertos[auxx].cursor, size, IDw);
							if (mq_send(mqwks[band3], msgresp, strlen(msgresp), 1) < 0)
								perror("mq_send2:worker");
								
							while (1)
							{
								memset(&msgbuf, 0,BUFF_SIZE*sizeof(char));
								memset(&msgresp, 0, BUFF_SIZE*sizeof(char));
								memset(&recv2, 0, 100*sizeof(char));
								
								// Recibimos datos del worker actual
								mq_receive(mqwk, msgbuf, BUFF_SIZE, &msgprio);
								
								if (msgprio == 6)
								{
									if (strcmp(msgbuf, "OK SIZE 0\n") == 0)
										Abiertos[auxx].cursor = Abiertos[auxx].cursor;
									else
										Abiertos[auxx].cursor = Abiertos[auxx].cursor + size;
										
									if(write(conn_s, msgbuf, strlen(msgbuf)))
											perror("REA:worker");
									break;
								}
								else
								{
									//printf("meti de nuevo el msj en la cola REA\n");
									if(mq_send(mqwk, msgbuf, strlen(msgbuf), msgprio) < 0) //Devuelvo el mensaje a la cola para no perderlo
										perror("mq_send: worker");
								}
							}
						}
					}
				}
			}
			/*------------------------------ CLO --------------------------------------*/
			if ((strcmp(recv[0], "CLO") == 0) && (strcmp(recv[1], "FD") == 0))
			{
				//printf("FUNCION CLO\n");
				conn_s = atoi(recv[cant - 1]); //socket
				
				if((atoi(recv[2]) == 0 && (strcmp(recv[2], "0") != 0))  || (cant-1 != 3) )
				{
					sprintf(msgresp, "ERROR %d EBADARG\n",Errores);
					pthread_mutex_lock(&lock2);
					Errores++;
					pthread_mutex_unlock(&lock2);
					if(write(conn_s, msgresp, strlen(msgresp)) < 0)
							perror("write:worker");	
				}
				else
				{
					auxx = atoi(recv[2]); //NFD
					
					if (strlen(Abiertos[auxx].name) > 0)
					{
						strcpy(Abiertos[auxx].name,"");
						Abiertos[auxx].idCliente = 0;
						Abiertos[auxx].idArchivoWorker =0;
						Abiertos[auxx].cursor =0;
						
						sprintf(msgresp,"OK\n");
					}
					else
					{
						sprintf(msgresp,"ERROR %d EOPNFILE\n",Errores);
						pthread_mutex_lock(&lock2);
						Errores++;
						pthread_mutex_unlock(&lock2);
					}
					
					if (write(conn_s, msgresp, strlen(msgresp))<0)
							perror("write:worker");
					
				}
			}
		    if ((strcmp(recv[0], "CLOT") == 0))
		    {
				conn_s = atoi(recv[1]);
				if(conn_s ==0 && strcmp(recv[1],"0") != 0)
				{
					sprintf(msgresp,"ERROR %d EBADARG\n",Errores);
					pthread_mutex_lock(&lock2);
					Errores++;
					pthread_mutex_unlock(&lock2);
					if (write(conn_s, msgresp, strlen(msgresp))<0)
							perror("write:worker");	
				}
				
				for (j = 0; j < NFD; j++)
					if(Abiertos[j].idCliente == conn_s)
					{
						strcpy(Abiertos[j].name,"");
						Abiertos[j].idCliente = 0;
						Abiertos[j].idArchivoWorker =0;
						Abiertos[j].cursor =0;
						if(j== NFD-1)
							NFD = NFD-1;
					}
			}	
		    /*----------------------------- CASO EN QUE SE HAYA ESCRITO MAL EL COMANDO ----------------------------------*/				
			if (((strcmp(recv[0], "REA") != 0) || (strcmp(recv[1], "FD") != 0) || (strcmp(recv[3], "SIZE") != 0) ) && ((strcmp(recv[0], "WRT") != 0) || (strcmp(recv[1], "FD") != 0) || (strcmp(recv[3], "SIZE") != 0) ) && (strcmp(recv[0],"DEL") != 0) && (strcmp(recv[0],"LSD") != 0) && (strcmp(recv[0], "OPN") != 0) && (strcmp(recv[0],"CRE") != 0) && ((strcmp(recv[0], "CLO") != 0) || (strcmp(recv[1], "FD") != 0)) && (strcmp(recv[0],"CLOT") != 0)  && msgprio == 0)
			{
				sprintf(msgresp,"ERROR %d EBADCMD\n",Errores);
				pthread_mutex_lock(&lock2);
				Errores++;
				pthread_mutex_unlock(&lock2);
				if(write(atoi(recv[cant - 1]), msgresp, strlen(msgresp) + 1) < 0)
						perror("write:worker");
			}
			
		}	
	}
	
    mq_close(mqwk);

	return 0;
}

void *handle_client(void *arg)
{
	char buffer0[BUFF_SIZE];
	char msgresp[BUFF_SIZE];
	char mq_name[BUFF_SIZE];
	int res, newid;
	long aux = (long)arg;
	int conn_s = aux;
	int IDw = 0;
	mqd_t mqwk;
	unsigned int msgprio = 0;
	char* recv[100];

	/* conn_s es el Socket del nuevo cliente conectado */
	
	// Nuevo cliente conectado; le asignamos un ID nuevo
	IDc++;
	newid = IDc;

	// Muestra en la terminal del server:
	fprintf(stderr, "Nuevo cliente %d conectado\n", IDc);

	// Leemos BUFF_SIZE bytes del cliente conectado (conn_s) y lo almacenamos en buffer. Devuelve el n° de bytes leídos.
	res = read(conn_s, buffer0, BUFF_SIZE);
	
	// En caso de no haber leído nada o en caso de que haya sucedio un error:
	if (res <= 0)
	{	
		// Escribe "Error al leer del buffer\n" en buffer2:
		sprintf(msgresp, "ERROR %d EBADCMD\n",Errores);
		pthread_mutex_lock(&lock2);
		Errores++;
		pthread_mutex_unlock(&lock2);
		write(conn_s, msgresp, strlen(msgresp));
		// Cerramos el cliente nuevo:
		close(conn_s);
		exit(-1);
	}
	
	memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
	// Mientras que no se haya escrito CON en el cliente (buffer):	
	while (strcmp(buffer0, "CON\r\n") != 0)
	{
		sprintf(msgresp, "ERROR %d EBADCMD\n",Errores);
		pthread_mutex_lock(&lock2);
		Errores++;
		pthread_mutex_unlock(&lock2);
		//Mostramos el error en la terminal del cliente (conn_s):
		write(conn_s, msgresp, strlen(msgresp));
		// Asignamos otro espacio de memoria para que escriba nuevamente:
		memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
		memset(&buffer0, 0,BUFF_SIZE*sizeof(char));
		// Leemos nuevamente:
		res = read(conn_s, buffer0, BUFF_SIZE);
	}
	
	/*-------------- CASO EN EL QUE EL CLIENTE HAYA ESCRITO CON ------------*/
	
	// Le asignamos al cliente nuevo conectado, un Worker aleatorio
	IDw = newid%5;
	
	if (IDw != 0) {
		// Mostramos el ID del Worker asignado:
		sprintf(msgresp, "OK ID %d\n", IDw);
		write(conn_s, msgresp, strlen(msgresp));
		
	
		// Abrimos la cola correspondiente al Worker (IDw):
		sprintf(mq_name, "/cola%d", IDw);

		mqwk = mq_open(mq_name, O_RDWR);
		
		// En caso de error al abrir la cola:
		if(mqwk < 0)
			perror("mq_open:handle_client");
	}
	else 
	{
		sprintf(msgresp, "OK ID %d\n", 5);
		write(conn_s, msgresp, strlen(msgresp));
	
		sprintf(mq_name, "/cola%d", 5);

		mqwk = mq_open(mq_name, O_RDWR);
		
		if(mqwk < 0)
			perror("mq_open:handle_client");
		
		// Comienza el anillo otra vez:	
		IDw = 1;
	}
	

	// Mientras que el cliente no cierre su conexión:
	while (1)
	{
		memset(&msgresp, 0,BUFF_SIZE*sizeof(char));
		memset(&buffer0, 0,BUFF_SIZE*sizeof(char));
		// Ese comando, lo guardamos en buffer. Devuelve la cantidad de bytes que leyó:
		res = read(conn_s, buffer0, BUFF_SIZE);
		if (strcmp(buffer0, "BYE\r\n") == 0 )
		{
			sprintf(recv[0], "CLOT %d", conn_s);
			if(mq_send(mqwk, recv[0], strlen(recv[0]), msgprio)<0)
				perror("mq_send: handle_client");
			break;
		}
		tokens(buffer0, recv, "\r"); //sacamos el \r\n que incorpora el telnet
		sprintf(recv[0], "%s %d", recv[0], conn_s);
		
		// Manda el siguiente comando al Worker con el que conectó: (no me queda claro lo de msgprio)
		if(mq_send(mqwk, recv[0], strlen(recv[0]), msgprio)<0)
			perror("mq_send: handle_client");

	}	
	
	if(mq_close(mqwk) < 0)
		perror("mq_close: handle_client");
    
    /*----------------- CERRAMOS CONEXION DEL CLIENTE ---------------*/
  	close(conn_s);
  	
	return 0;
}


int main(int argc, char **argv)
{
	/* Unlink de las colas */
	mq_unlink("/cola1");
	mq_unlink("/cola2");
	mq_unlink("/cola3");
	mq_unlink("/cola4");
	mq_unlink("/cola5");

	
	// Designamos un hilo nuevo para un nuevo cliente
	pthread_t clienteNuevo;
	
	// Designamos 5 hilos para los 5 workers independientes
	pthread_t workers[5];
	
	// Declaramos las variables para la conexión
	long t;
	int list_s,conn_s = -1;
	struct sockaddr_in servaddr;
	long arg;
  
	// Designamos el puerto
	int PORT = atoi(argv[1]);
  
  
	/* Creación de las colas de mensajes para cada uno de los Workers */
	mq_buzon.mq_maxmsg = 100;
	mq_buzon.mq_msgsize = 128;
	mq_buzon.mq_flags = 0;
  
	// Abrimos e inicializamos las colas
	mqwk1 = mq_open("/cola1", O_CREAT, S_IRWXU, NULL);
	if(mqwk1<0)
		perror("mq_open_1");
	mqwk2 = mq_open("/cola2", O_CREAT, S_IRWXU, NULL);
	if(mqwk2<0)
		perror("mq_open_2");
	mqwk3 = mq_open("/cola3", O_CREAT, S_IRWXU, NULL);
	if(mqwk3<0)
		perror("mq_open_3");
	mqwk4 = mq_open("/cola4", O_CREAT, S_IRWXU, NULL);
	if(mqwk4<0)
		perror("mq_open_4");
	mqwk5 = mq_open("/cola5", O_CREAT, S_IRWXU, NULL);
	if(mqwk5<0)
		perror("mq_open_5");


	/* Creamos los 5 hilos diferentes para cada uno de los workers */
	for(t = 1; t < 6; t++)
		pthread_create(&workers[t], NULL, worker, (void *)t);


	if ((list_s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "ECHOSERV: Error creating listening socket.\n");
		return -1;
	}

  
	memset(&servaddr, 0, sizeof(servaddr));
	servaddr.sin_family      = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port        = htons(PORT);

	/* Conexiones de un nuevo cliente */
	if (bind(list_s, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0) {
		fprintf(stderr, "ECHOSERV: Error calling bind()\n");
		return -1; 
	}

	if (listen(list_s, 10) < 0) {
		fprintf(stderr, "ECHOSERV: Error calling listen()\n");
		return -1;                          
	}


	// Esperamos a que se conecte un cliente nuevo y se lo asignamos al hilo clienteNuevo
	while (1) 
	{
		if ((conn_s = accept(list_s, NULL, NULL)) < 0) 
		{
			fprintf(stderr, "ECHOSERV: Error calling accept()\n");
			return -1;
		}	
	// Socket nuevo del clienteNuevo
	arg = conn_s;
	// Le asignamos un nuevo hilo y vamos a handle_client
    pthread_create(&clienteNuevo, NULL, handle_client, (void *)arg);
	}
  
  
	/* Cerramos las colas */
	if(mq_close(mqwk1)<0)
		perror("mq_close:main");
	if(mq_close(mqwk2)<0)
		perror("mq_close:main");
	if(mq_close(mqwk3)<0)
		perror("mq_close:main");
	if(mq_close(mqwk4)<0)
		perror("mq_close:main");
	if(mq_close(mqwk5)<0)
		perror("mq_close:main");

		
  
	return 0;

}

