## Almacenamiento y Procesamiento Masivo de Datos - Tarea 2 
**Universidad de los Andes**

**Alumno:** Cristóbal Griffero

### Objetivo

La tarea dos tiene por objetivo profundizar en algunas consultas utilizando Map Reduce. Para esto, se utilizó el Dataset de
Yelp en formato JSON.

### Preparación

La tarea se realizó utilizando python en conjunto con [mrjob](https://github.com/Yelp/mrjob).

En primera instancia se redujo el dataset con el objetivo de disminuir los tiempos de ejecución con el objetivo de poder
probar eficientemente el código

```
head -n 3000 user.json > user_short.json
```
```
head -n 3000 review.json > review_short.json
```
```
head -n 3000 business.json > business_short.json
```

Cabe mencionar que tomar los primeros 3000 no es la mejor manera de reducir el dataset ya que se produce un sesgo importante
en los resultados.

Con respecto a los benchmarks, se utilizó [time](https://docs.python.org/2/library/time.html) para medir los tiempos de
ejecución.

### P1

**Pregunta:** Usando mrjob, encuentre el comentario (review) "más único": Consiste en el comentario que use la mayor cantidad
de palabras usadas solo una vez en todo el
dataset. Utilice un split simple, por espacios, removiendo antes todos los signos de puntuación.

```
python p1.py review_short.json
```

Para esta pregunta, se utilizó 1 map y 3 reduce. El funcionamiento de la consulta es en primer lugar realizar un map 
entre cada palabra de un review con el id del review mismo. Luego con un reduce, para cada palabra, se revisa si esta es única
en todos los reviews. Si es unica se emite el id_review, 1 y luego otro reduce agrupa la cantidad de palabras únicas por cada
review. Finalmente un último review busca el máximo determinando así el review más único.

**Resultados**

| Tamaño Dataset |    Review más único    | Cantidad de palabras únicas | Tiempo de ejecución |
| -------------: |:----------------------:| ---------------------------:|--------------------:|
|          10.000| uuvlg4oRv4f6p7gB418BVQ |                          111|               19.6 s|
|          30.000| kkit_FA06i3Ft3-80Al9_w |                          122|               55.2 s|


### P2

**Pregunta:** Usando mrjob, encuentre los usuarios más similares según el índice de Jaccard
(https://en.wikipedia.org/wiki/Jaccard_index). (Similaridad mayor a 0.5)


```
python p2.py business_short.json
```

Para la pregunta nº2 se utilizaron 3 maps y 3 reduce. Primero se hace un map entre user y business. Luego un reduce arma una nueva key que contiene el usuario y la cantidad de reviews que ha hecho y como value toma una lista de business donde ha hecho review. Posteriormente, se hace un map business - user, amount of reviews. En el siguiente reduce, utilizando [itertools](https://docs.python.org/2/library/itertools.html) se hace la combinatoria entre todos los pares de usuarios que hicieron review sobre el mismo lugar. Con esto ya se tiene toda la información necesaria para calcular el índice de Jaccard. El siguiente map y reduce simplemente obtienen el indice de Jaccard cuando este es mayor a 0.5

**Resultados**

| Tamaño Dataset | Tiempo de ejecución |
| -------------: |--------------------:|
| 10.000         |              137.6 s|
| 30.000         |              396.2 s|

Con el conjunto de datos utilizado, se puede evidenciar que existen muchos pares de usuarios con índice de Jaccard = 1. Esto se debe a que existen usuarios que solamente hicieron reviews sobre un establecimiento por lo que cualquier par de usuarios que hacen reviews de un solo establecimiento son iguales en términos del índice de Jaccard.

### P3

**Pregunta:** Usando mrjob, encuentre los usuarios con más reviews por cada categoría, y luego
pondere los resultados según los votos recibidos en el comentario.


```
python p3.py business_short.json
```

Para la pregunta nº2 se utilizaron 3 maps y 3 reduce. Primero se hace un map entre user y business. Luego un reduce arma una nueva key que contiene el usuario y la cantidad de reviews que ha hecho y como value toma una lista de business donde ha hecho review. Posteriormente, se hace un map business - user, amount of reviews. En el siguiente reduce, utilizando [itertools](https://docs.python.org/2/library/itertools.html) se hace la combinatoria entre todos los pares de usuarios que hicieron review sobre el mismo lugar. Con esto ya se tiene toda la información necesaria para calcular el índice de Jaccard. El siguiente map y reduce simplemente obtienen el indice de Jaccard cuando este es mayor a 0.5

**Resultados**

| Tamaño Dataset | Tiempo de ejecución |
| -------------: |--------------------:|
| 10.000         |              137.6 s|
| 30.000         |               55.2 s|

Con el conjunto de datos utilizado, se puede evidenciar que existen muchos pares de usuarios con índice de Jaccard = 1. Esto se debe a que existen usuarios que solamente hicieron reviews sobre un establecimiento por lo que cualquier par de usuarios que hacen reviews de un solo establecimiento son iguales en términos del índice de Jaccard.






