# Curso de Azure para Ingenieros de datos
***
## *Exámen Final*
*Autor:* Danilo Oviedo

*Creado:* 22/06/2021
***
## Problematica:
Dado el siguiente modelo analítico.
 
El departamento de Marketing, desea enviar una campaña personalizada a cada uno de los clientes, promocionando el producto que más veces han solicitado. Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analítica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la última compra
- Correo del cliente

Las tablas del modelo analítico proveen los datos transaccionales. Sin embargo los correos electrónicos han sido proporcionados por medio de un archivo CSV el cual se encuentra adjunto.

![image](https://user-images.githubusercontent.com/67159200/175715268-7a927afe-64ef-4ae8-ba29-f4dfc20c9879.png)


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
***
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
  #### 6. escoger el destino que sera nuestro storage configurado previamente
     en la configuración se ingresara la "Ruta de acceso de la carpeta"; esta configuramos previamente para guardar rn raw/doviedo
  ![image](https://user-images.githubusercontent.com/67159200/175755244-1742205a-7291-48dc-8946-c425aaf786b9.png)
  8. Configurar formato de archivos; es importante recalcar que lo mas optimo es usar parquet y snappy
  ![image](https://user-images.githubusercontent.com/67159200/175755270-bfbcb125-2278-4621-afba-22889dec547b.png)
  9. revisaremos un resumen de todo lo configurado y finalizamos (esperar hasta que se ejecute el pipeline)
  ![image](https://user-images.githubusercontent.com/67159200/175755361-7c23615d-e65b-4feb-82c2-20f34c2600e4.png)
***
El Archivo Cliente y Producto se crearon bajo este esquema para el de Factura; al ser una tabla que va a ir creciendo en el tiempo tiene una seccion de configuracion diferente
***

***
***
***
***
![image](https://user-images.githubusercontent.com/67159200/175699066-dd570642-1d38-497e-864e-a507b3ec8cb9.png)
1. dar click ene l menú de la izquierda en el icono seccion "integrar"
2. click en el icono "+" para agregar un nuevo canal
3. poner el nombre según lo solicitado: "doviedo_pipeline"
4. en seccion "actiidades" arrastrar el ícono "Copiar datos"
