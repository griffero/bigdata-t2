## Almacenamiento y Procesamiento Masivo de Datos - Tarea 2 
**Universidad de los Andes**

**Alumno:** Cristóbal Griffero

### Objetivo

La tarea dos tiene por objetivo profundizar en algunas consultas utilizando Map Reduce. Para esto, se utilizó el Dataset de Yelp en formato JSON.

### Preparación

La tarea se realizó utilizando python en conjunto con [mrjob](https://github.com/Yelp/mrjob).

En primera instancia se redujo el dataset con el objetivo de disminuir los tiempos de ejecución con el objetivo de poder probar eficientemente el código

```
head -n 3000 user.json > user_short.json
```
```
head -n 3000 review.json > review_short.json
```
```
head -n 3000 business.json > business_short.json
```

Cabe mencionar que tomar los primeros 3000 no es la mejor manera de reducir el dataset ya que se produce un sesgo importante en los resultados.

Con respecto a los benchmarks, se utilizó [time](https://docs.python.org/2/library/time.html) para medir los tiempos de ejecución.

### P1

**Pregunta:** Usando mrjob, encuentre el comentario (review) “más único”: Consiste en el comentario que use la mayor cantidad de palabras usadas solo una vez en todo el
dataset. Utilice un split simple, por espacios, removiendo antes todos los signos de puntuación.

Para esta pregunta, se utilizó 1 map y 3 reduce. El funcionamiento de la consulta es en primer lugar realizar un map entre cada palabra de un review con el id del review mismo. Luego con un reduce, para cada palabra, se revisa si esta es única en todos los reviews. Si es unica se emite el id_review, 1 y luego otro reduce agrupa la cantidad de palabras únicas por cada review. Finalmente un último review busca el máximo.

