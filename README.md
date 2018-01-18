# Servidor de archivos

Un sistema de archivos (o FS) distribuido o sistema de archivos de red es un sistema de archivos de computadoras que sirve para compartir archivos, impresoras y otros recursos como un almacenamiento persistente en una red de computadoras. El trabajo consiste en implementar un sistema de archivos distribuidos simplificado con un set de operaciones mı́nimas.

Este servidor, tiene la capacidad de que, múltiples clientes, puedan abrir un mismo archivo y acceder al mismo en forma de lectura y escritura; aparte de todas las funcionalidades pedidas por el enunciado del proyecto. Estas son CON, LSD, DEL, CRE, OPN, WRT, REA, CLO y BYE.

# Trabajo Final de Sistemas Operativos 1 de Licenciatura en Ciencias de la Computación de la UNR

# Forma de ejecución en C:
Nuestro servidor escucha un puerto y se comunica mediante telnet. Con lo
cual, supongamos que el archivo ejecutable se llame server, entonces en una
terminal pondríamos lo siguiente: ./server 8000, donde 8000 es un puerto.
Ahora, para conectar un cliente haríamos lo siguiente: abrir otra terminal, y
escribir telnet localhost 8000. De esta forma, podríamos conectar múltiples
clientes.

# Forma de ejecución en Erlang:
El código se llama serverErlang.erl, para ejecutarlo se deben seguir los
siguientes pasos: en una terminal, abrir Erlang y compilar el código; ejecutar
serverErlang:listen(8000) (donde 8000 es un puerto). Luego, abrir con telnet
otra terminal de la misma forma que en Posix.
