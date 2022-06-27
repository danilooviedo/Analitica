# Curso de Azure para Ingenieros de datos
***
## *Exámen Final*
*Autor:* Danilo Oviedo

*Creado:* 22/06/2021
***
## Problematica:
Dado el siguiente modelo analítico.
![image](https://user-images.githubusercontent.com/67159200/175715268-7a927afe-64ef-4ae8-ba29-f4dfc20c9879.png) 
El departamento de Marketing, desea enviar una campaña personalizada a cada uno de los clientes, promocionando el producto que más veces han solicitado. Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analítica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la última compra
- Correo del cliente

Las tablas del modelo analítico proveen los datos transaccionales. Sin embargo los correos electrónicos han sido proporcionados por medio de un archivo CSV el cual se encuentra adjunto.
***
## Lineamientos generales:

1. El nombre de usuario debe ser la letra inicial del primer nombre y su apellido Ejm: Agustin Martinez (amartinez)
2. Cada pipeline de ADF debe anteponer el nombre de usuario seguido de pipeline Ejm: amartinez_pipeline
3. Los archivos ingestados en ADL2 deben colocarse en RAW en una carpeta con el nombre de usuario
4. Los notebooks de Spark deben ser nombrados  {usuario}_notebook
5. La tabla de resultados debe almacenarse en un POOL SQL, en el esquema default con el esquema "tbl_{usuario}"

Finalmente, cada participante deberá elaborar un archivo Markdown en Github con acceso publico. Por su puesto en el documento debe obviarse datos sensibles como claves etc.
El documento Markdown deberá contener el proceso técnico que el participante ha seguido con un orden lógico y el link del proyecto Githab deberá ser registrado como entregable de esta tarea.
***
***
# Solución
***
### 1. El nombre de usuario debe ser la letra inicial del primer nombre y su apellido Ejm: Agustin Martinez (amartinez)
nombre de usuario: dovido

#### NOTA: 
Es necesario realizar primero el punto 3 ya que aqui se guardara toda la informacion que se ala desde los pipeline.
***
### 3. Los archivos ingestados en ADL2 deben colocarse en RAW en una carpeta con el nombre de usuario
Para crear la carpeta se debe realizar desde el portal de azure e ingresar a los recursos de tipo "Cuena de Almacenamiento"
![image](https://user-images.githubusercontent.com/67159200/175754857-5309c1f5-7d48-457d-bfd2-08b6905377de.png)
una vez dentro debemos ir a la sección "Almacenamiento de Datos" => "Contenedores"
![image](https://user-images.githubusercontent.com/67159200/175754888-c5cba07a-ed65-42a3-908d-73e786e0b800.png)
Lugo navegaremos en las carpetas hasta llegar a raw; damos click en "+ Agregar directorio" y en este ponemos el nombre de la carpeta; para mi caso en base a lo solicitado es "doviedo" y damos click en crear
![image](https://user-images.githubusercontent.com/67159200/175754925-60900719-aea4-4dc5-bb97-a38b6df07ff3.png)
****
### 2. Cada pipeline de ADF debe anteponer el nombre de usuario seguido de pipeline Ejm: amartinez_pipeline
Para llegar a crear un pipeline de tipo EL debemos previamente tener configurado las bases de Origen y destino; a continuación se explica como crearlas.

##### Crear LinkedServer Origen; para el ejercicio hacemos linked a un servidor onpremise
- Los pasos a seguir son:
    Dar click en "Administrar" => "Servicios Vinculados" => "+ Nuevo" => escoger el motor de base de datos; para este caso es MySQL
 ![image](https://user-images.githubusercontent.com/67159200/175752946-53f1c3a5-bb42-4957-9b99-b2053413e019.png)
 
 - Una vez seleccionado el motor de Base de Datos se debe configurar con las credenciales y para validar damos click en "Prueba de Conexión"; si todo va bien damos click en "Crear" y listo!

 ![image](https://user-images.githubusercontent.com/67159200/175753099-a5f05a60-8447-4c78-bc46-c5eedf315496.png)

 

##### Crear Linked Server Destino; se creará un storage en Azure
- Los pasos a seguir para escoger el storage de Azure son los mismo que en el paso anterior solo que en vez de escoger MySQL vamoz a seleccionar "Azrue Data Lake Storage Gen2"; ya que es la mas optimizada para analítica de datos
![image](https://user-images.githubusercontent.com/67159200/175753404-a6bc0de3-837a-419f-9729-625c176e7a1f.png)
- Para configurar se realiza los siguientes pasos

![image](https://user-images.githubusercontent.com/67159200/175753819-8916c67c-ef58-43f6-b9df-71bd41358b81.png)
##### Nota:
No puedo agragar porque no tengo las credenciales; para el ejercicio ocupare un ADL existente

Para crear el pipeline se siguió los siguientes pasos:
  1. se debe estar en el menu "Inicio " y dar click en "Ingerir"
  ![image](https://user-images.githubusercontent.com/67159200/175755017-b5b324c1-d1df-462a-9e47-6fea03ec46e1.png)
  2. escoger "tarea de copia integrada" => Siguiente
  ![image](https://user-images.githubusercontent.com/67159200/175755057-ec220cdd-3334-4080-a799-f4ca096a87a9.png)
  #### 3. configurar "Tipo de origen", "Conexión" (esta será el origen que configuramos previamente) => escoger la tabla de la que se extraerá la data => siguiente
  ![image](https://user-images.githubusercontent.com/67159200/175755133-d0b6aa14-0300-4e30-a336-c4c4db36d323.png)
  4. podemos ver una vista previa de la data a cargar
  ![image](https://user-images.githubusercontent.com/67159200/175755146-f890d62f-b03e-40f4-81ba-a8bc325d657e.png)
  #### 5. escoger el destino que sera nuestro storage configurado previamente
     en la configuración se ingresara la "Ruta de acceso de la carpeta"; esta configuramos previamente para guardar rn raw/doviedo
  ![image](https://user-images.githubusercontent.com/67159200/175755244-1742205a-7291-48dc-8946-c425aaf786b9.png)
  6. Configurar formato de archivos; es importante recalcar que lo mas optimo es usar parquet y snappy
  ![image](https://user-images.githubusercontent.com/67159200/175755270-bfbcb125-2278-4621-afba-22889dec547b.png)
  
  7. damos un nombre al pipeline con el formaro del nombre en base a lo solicitado 
  ![image](https://user-images.githubusercontent.com/67159200/175755993-227b9540-ef49-4180-a4cd-fbce473521fe.png)
  8. revisaremos un resumen de todo lo configurado 
  ![image](https://user-images.githubusercontent.com/67159200/175756008-e0a25d4b-28d2-4882-a888-363bae31e5c5.png)
  9. Finalizamos (esperar hasta que se ejecute el pipeline)
  ![image](https://user-images.githubusercontent.com/67159200/175755361-7c23615d-e65b-4feb-82c2-20f34c2600e4.png)
***
El Archivo Cliente y Producto se crearon bajo este esquema para el de Factura y FacturaProducto; al ser una tabla que va a ir creciendo en el tiempo (carga append) tiene una seccion de configuracion diferente
***
#### Pipeline para archivo  Factura y FacturaProducto
Para esto se crea previamente la carpeta "factura" dentro de la carpeta "doviedo" en el contenedor del almacenamiento de datos para aqui poder ir cargando varios archivos parquet de factura por cada mes

Para la creación del pipeline de factura solo se tiene una variacion y es en el paso de "conjunto de datos" al querer halar la data se escogera la opción "utilizar consulta" y nos aparecerá un recuadro donde ingresaremos el script para cargar el mes necesario
![image](https://user-images.githubusercontent.com/67159200/175756870-8477fe5b-7438-4430-b1c1-ace2780a4eca.png)
los siguientes pasos seran los mismos tomando en cuenta lo mencionado antes que factura se guardara en una subcarpeta ya que tendrá la informacion de cada mes separado en diferentes archivos parquet.
#### Nota:
si en un futuro se quiere consultar todos los archivos parquet de factura sera suficiente con llamar a la carpeta y automaticamente se hara un tipo union entre los archivos que estan dentro siempre que tenga la misma estructura.

Para evitar crear un pipeline por cada mes de carga de facturación procedemos a modificar el pipeline
 1. Arrastramos la actividad "ForEach"
 2. Cortamos la actividad "copiar datos" => damos click en la caja "ForEach" en el icono editar activities; esto nos llevara a una subpantalla
 ![image](https://user-images.githubusercontent.com/67159200/175757678-c038503c-5e6d-466f-b0a3-83e2c5b568c8.png)
 3. pegamos la actividad que se encarga de cargar data de factura; para regresar al pipeline donde esta el ForEach damos click en el nombre del pipeline
 ![image](https://user-images.githubusercontent.com/67159200/175757723-0a6c3699-4eca-4b23-b2a4-2070e5df93a5.png)
 4. editar el source donde se guardara el archivo parquet de la factura para que sea parametrizabe; para eso agregamos un nuevo parametro
 ![image](https://user-images.githubusercontent.com/67159200/175758476-cc637b1a-3cda-4e1d-8b82-96f37c71d596.png)
 5. cramos variables una sera un arreglo de meses en formato json y la otra se asignara cada vez que recorre en el ForEach tomando solo un registro
 variable vAnioMeses
 ```
 [{"aniomes":"202201"},{"aniomes":"202202"},{"aniomes":"202203"},{"aniomes":"202204"},{"aniomes":"202205"}]
 ```  
 6. configuramos el "ForEach" para que recorra el arreglo json
 ![image](https://user-images.githubusercontent.com/67159200/175758872-380e044b-228d-4d3b-8135-da714f0c26b4.png)
 7. arrastramos la actividad "establecer variable" y la configuramos para que tome el valor del ForEach.
 8. Canbiamos la configuracion de la actividad de copia para que el script de consulta sea dinámico
 ![image](https://user-images.githubusercontent.com/67159200/175759015-4af1ef86-798f-4e5f-8a16-8c61663a2e7f.png)
 10. Para correr el pipeline damos click en "Depurar", mientras se ejecuta no mostrara una ventana del status
 ![image](https://user-images.githubusercontent.com/67159200/175762503-8088c112-fcac-40cb-b03a-eab02431f736.png)

***
### Otra forma de crear un piipeline es ir directo a la pantalla de integrar
***
![image](https://user-images.githubusercontent.com/67159200/175699066-dd570642-1d38-497e-864e-a507b3ec8cb9.png)
1. dar click ene l menú de la izquierda en el icono seccion "integrar"
2. click en el icono "+" para agregar un nuevo canal
3. poner el nombre según lo solicitado: "doviedo_pipeline"
***
### 4. Los notebooks de Spark deben ser nombrados  {usuario}_notebook
Para crear un notbook (pnatalla en la que se ingresa codigo para desarrollar) se debe seguir los siguientes pasos:
dar click en "Desarrollar" => click en "+" para agregar nuevo => clic en notebook
![image](https://user-images.githubusercontent.com/67159200/175762851-60435563-ec5c-4363-9205-6ec112af46bb.png)

***
***
# Código pyspark para realizar la consulta solicitada que solventa la problematica planteada:
Para comprobar que el script funcione correctamente he cogido de ejemplo el cliente: recbh5oITyJl9SNGK

importar librerias a usar
```
%%pyspark
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import explode
from pyspark.sql.functions import concat,col
```
Leer el archivo .csv y guardar en variable DataFrame
```
dfClienteCorreo = spark.read.load('abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/doviedo/clientes_correos.csv', format='csv')
display(dfClienteCorreo.filter(dfClienteCorreo._c0=='recbh5oITyJl9SNGK').limit(100))
```
Cargar los archivos parquet que importamos en pasos anteriores y guardar en variable DataFrame
```
vPath = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/doviedo/'
dfCliente = spark.read.load(vPath + 'cliente.parquet', format='parquet')
dfProducto = spark.read.load(vPath + 'producto.parquet', format='parquet')
dfFactura = spark.read.load(vPath + 'factura/*.parquet', format='parquet')
dfFactProducto = spark.read.load(vPath + 'factura_producto/*.parquet', format='parquet')
```
Consultar todos los productos comprados por cada cliente
```
dfClienteProductos = dfFactura\
                .join(dfCliente, dfCliente.rowidcliente == dfFactura.rowidcliente)\
                .join(dfFactProducto, dfFactura.rowidfactura == dfFactProducto.rowidfactura)\
              .select(dfCliente.rowidcliente, dfFactProducto.rowidproducto)
```
Contar cuantos productos ha comprado cada cliente
```
dfNumProductosCliente = dfClienteProductos.groupBy('rowidcliente','rowidproducto').count() 

```
Consultar el producto más vendido a cada cliente
```
dfTemp= dfNumProductosCliente.groupBy("rowidcliente").agg(F.max("count"))
dfProductoMasVendidoCliente = dfNumProductosCliente.alias('t1')\
                                .join(dfTemp.alias('t2'), (F.col('t1.rowidcliente') == F.col('t2.rowidcliente')) 
                                                        & (F.col('t1.count') == F.col('t2.max(count)')))\
                                .select(F.col('t1.rowidcliente'),F.col('t1.rowidproducto'), F.col('t1.count'))
```
Validamos el script anterior para comprobar que el producto más vendido es: #recbh5oITyJl9SNGK - Limpieza profunda - 19 y no Limpieza express porque tiene 9
```
display(dfClienteProductos.filter( (dfClienteProductos.rowidcliente  == "recbh5oITyJl9SNGK")))
display(dfProductoMasVendidoCliente.filter(dfProductoMasVendidoCliente.rowidcliente=='recbh5oITyJl9SNGK').limit(100))
```
### NOTA
No se puede agrupar por mas de una columna por eso es necesario hacer un match contra la tabla inicial para sacar el detalle necesitado; el siguiente sctipt no funcionó
```
#dfProductoMasVendidoCliente= dfNumProductosCliente.groupBy("rowidcliente",'rowidproducto').agg(F.max("count"))  # => no funciona
#dfProductoMasVendidoCliente= dfNumProductosCliente.groupBy("rowidcliente",'rowidproducto').agg(F.max("count(rowidproducto)").alias("Valor"))
```
consultar todas las veces que el cliente compró el mismo producto (más vendido)
```
dfFacturasProductoCliente = dfProductoMasVendidoCliente.alias('tp')\
            .join(dfFactura.alias('tf'), F.col('tp.rowidcliente') == F.col('tf.rowidcliente'))\
            .join(dfFactProducto.alias('tfp'), ((F.col('tf.rowidfactura') == F.col('tfp.rowidfactura')) 
                                    & (F.col('tp.rowidproducto') == F.col('tfp.rowidproducto'))
                                  )
                  )\
            .select(F.col('tp.rowidcliente'), 
                    F.col('tp.rowidproducto'),
                    F.col('tf.rowidfactura'), 
                    F.col('tfp.fecha'))

display(dfFacturasProductoCliente.filter(dfFacturasProductoCliente.rowidcliente=='recbh5oITyJl9SNGK').limit(100))

```
Encontrar la última fecha que el cliente compró el producto más vendido
```
dfMaxFechaCompraProducto= dfFacturasProductoCliente.groupBy("rowidcliente").agg(F.max("fecha").alias('ultimacompra'))
```
## Resultado Final 
```
dfResult = dfFacturasProductoCliente.alias('t1')\
            .join(dfMaxFechaCompraProducto.alias('t2'), 
                    (F.col('t1.rowidcliente') == F.col('t2.rowidcliente')) 
                  & (F.col('t1.fecha') == F.col('t2.ultimacompra'))
                )\
            .join(dfProducto.alias('tp'),
                    F.col('t1.rowidproducto') == F.col('tp.rowidproducto')
                )\
            .join(dfClienteCorreo.alias('tcc'),
                    F.col('t1.rowidcliente') == F.col('tcc._c0')
                )\
            .select(F.col('t1.rowidcliente'), 
                    F.col('t1.rowidproducto'), 
                    F.col('tp.producto'), 
                    F.col('tcc._c1').alias('correo'), 
                    F.col('t2.ultimacompra'))
display(dfResult.filter(dfResult.rowidcliente=='recbh5oITyJl9SNGK').limit(100))
``` 
***
cargar en base


