# Air Traffic Control [![Generic badge](https://img.shields.io/badge/Spark-2.2.0,%202.3.0-green.svg)](https://shields.io/) [![Generic badge](https://img.shields.io/badge/Scala-2.11.8-green.svg)](https://shields.io/)

Un aeropuerto quiere optimizar el orden en el que pueden aterrizar los aviones por minuto. De
esta forma genera por minuto un fichero con todos los aviones que se encuentran en el aire y
que quieren aterrizar. Este fichero debe procesarse para mostrar, lo más rápido posible, los
aviones con el orden de aterrizaje, intentando minimizar el tiempo que tarda en realizarse la
operación, y dentro de lo posible el número de pasajeros en el aire.
#### Se pide que se diseñe e implemente un programa que resuelva este problema, con los siguientes requerimientos funcionales:
1. El aeropuerto tiene dos pistas de aterrizaje, pero puede aceptar 2 aviones pequeños o
1 avión grande a la vez.
2. Para aterrizar cada avión pequeño emplea 6 segundos mientras que cada avión
Grande emplea 9 segundos.
3. Solo se deben mostrar aquellos aviones que vayan a aterrizar, aquellos que no puedan
por disponibilidad se quedarán en memoria.
#### Y con los siguiente Requerimientos no funcionales:
1. El servicio debe ser consistente, produciendo siempre la misma salida ante la misma
entrada.
2. Debemos tener un log con el registro.