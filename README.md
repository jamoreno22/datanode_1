# datanode_1

Se escoge cargar (0):
    Se escoge distribución centralizada (0):
        Se escoge el libro ingresando su nombre:
            Se genera una propuesta en el DataNode y se envía al Namenode
            Este la acepta o genera una nueva propuesta
            Se escribe el LOG
            Se envían las propuestas a los DataNode y estos guardan los chunks correspondientes
            print todo ta bien de pana banana
    Se escoge distribución distribuida (1):
        Se escoge el libro ingresando su nombre:
            Se genera una propuesta en el DataNode, si es aceptada se envía al NameNode para que se
            escriba el LOG (si hay conflictos se solucionan con el algoritmo Ricart-Agrawala)
            Se guardan los chunks correspondientes en los DataNodes
            OMEDETO


Se escoge descargar (1):
    Se escoge el nombre del libro:
        Se solicitan las ubicaciones al NameNode
        Estas son retornadas al Cliente
        Luego el cliente solicita los chunks a los DataNodes correspondientes
        Los chunks son entregados al cliente
        El libro se reconstruye a partir de los chunks recibidos.
        Este es guardado en la carpeta /LibrosDelCliente

        Crear la lógica para descargar, utilizar la ip y el nombre del chunk entregados para encontrar estos en el datanode y enviarselos al cliente.