from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random


N = 100
K = 10
NPROD = 3
NCONS = 3


def delay(factor = 3):
    sleep(random()/factor)


def add_data(storage, index, data, mutex):
    mutex.acquire()
    try:
        storage[index.value] = data
        delay(6)
        index.value = index.value + 1
    finally:
        mutex.release()


def get_data(storage, index, mutex):
    mutex.acquire()
    try:
        data = storage[0]
        index.value = index.value - 1
        delay()
        for i in range(index.value):
            storage[i] = storage[i + 1]
        storage[index.value] = -1

    finally:
        mutex.release()
    return data

def get_lower(storage):
    # index = proceso con el menor numero
    index = -1
    # value = menor valor de entre los procesos
    value = None
    for i in range(NPROD):
        if storage[i][0] == -1:
            continue

        # Si no han sido incializadas las variables se asignan al primer valor encontrado
        # Siempre i = 0 y el primer valor del storage del prod_0
        if index == -1:
            index = i
            value = storage[i][0]

        # Si el valor leido es menor que el que contemplabamos como menor
        # actualizamos value e index
        if storage[i][0] < value:
            value = storage[i][0]
            index = i

    # En este punto, value tiene el menor valor disponible e index el proceso que lo tiene
    return index

def finished(storage):
    is_finished = True
    for i in range(NPROD):
        if storage[i][0] != -1:
            is_finished = False
            break
    return is_finished


def producer(storage, index, empty, non_empty, mutex):
    for v in range(N):
        delay(6)
        empty.acquire()
        add_data(storage, index, v, mutex)
        non_empty.release()
        print (f"producer {current_process().name} almacenado {v}")
    empty.acquire()
    add_data(storage, index, -1, mutex)
    non_empty.release()
    print (f"producer {current_process().name} terminado")


def consumer(storageList, indexList, emptyList, non_emptyList, mutexList, final):
    acquireList = []
    acquireList = [False for i in range(NPROD)]
    indice = 0
    while True:
        for idx in range(len(acquireList)):
            if not acquireList[idx]:
                non_emptyList[idx].acquire()
                acquireList[idx] = True

        # Si estamos aqui es porque hay datos disponibles de los 3 productores
        # Si ya tenemos todos los storages con -1 terminamos el bucle
        if finished(storageList): break
        i = get_lower(storageList)
        acquireList[i] = False
        dato = get_data(storageList[i], indexList[i], mutexList[i])
        emptyList[i].release()
        final[indice] = dato
        indice += 1
        print (f"Desalmacenando del proceso {i} el valor {dato}")
        delay()

def main():
    # Elementos de cada productor
    storageList = []
    indexList = []
    non_emptyList = []
    emptyList = []
    mutexList = []
    prodList = []

    # Lista final ordenada
    final = Array('i', NPROD*N)

    for i in range(NPROD):
        # Creamos un aux_storage y lo inicializamos con -1 en sus valores
        aux_storage = Array('i', K)
        for i in range(K):
            aux_storage[i] = -1
        
        # Introducimos ese storage a la lista de storages
        storageList.append(aux_storage)
        
        indexList.append(Value('i', 0))
        non_emptyList.append(Semaphore(0))
        emptyList.append(BoundedSemaphore(K))
        mutexList.append(Lock())

    for i in range(NPROD):
        print(i)
        prodList.append(Process(target=producer,
                        name=f'prod_{i}',
                        args=(storageList[i], indexList[i], emptyList[i], non_emptyList[i], mutexList[i])))

    merge = Process(target=consumer,
                      name=f"merge",
                      args=(storageList, indexList, emptyList, non_emptyList, mutexList, final))

    for p in prodList:
        p.start()
    merge.start()

    for p in prodList:
        p.join()
    merge.join()

    print ("almacen final", final[:])


if __name__ == '__main__':
    main()