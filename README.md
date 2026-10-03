# Taller de Sistemas Transaccionales semana 6

Autor: Carlos Alberto Palencia Pombo. Ejercicios de mensajería, caché distribuida, procesamiento de eventos y coordinación 2PC con Java y MySQL.

## Requisitos

JDK 17 o 21, MySQL Server 8.0, Maven, NetBeans o IntelliJ. Internet para la primera descarga de dependencias. No se necesita instalar por separado ActiveMQ, Hazelcast ni un clúster Storm: el proyecto inicia los componentes locales reales usando sus bibliotecas Java. Storm LocalCluster es una demostración local, no un despliegue de producción.

## Base de datos

Abra `sql/backup_inicial_s6.sql` en MySQL Workbench y ejecútelo. Crea los esquemas `sistemas_transaccionales_s6` y `sistemas_transaccionales_s6_destino`. Es repetible: no reinicia saldos ya modificados.

El paquete complementario local del taller incluye `sql/backup_resultados_s6.sql`, un respaldo mysqldump de los dos esquemas después de las pruebas: saldos 850/650, eventos de mensajería y caché, y seis operaciones Storm correspondientes a dos ejecuciones de tres eventos. La segunda comprobó además la finalización de la JVM. Este respaldo no se publica en GitHub. Úselo en un entorno de práctica: restaura las tablas de esos dos esquemas. Para reproducir exactamente 1000/500, use los datos iniciales en esquemas nuevos. Cada ejecución exitosa de 2PC vuelve a transferir 150; no suponga que ejecutar `all` deja siempre 850/650.

## Configuración

Configure las variables en la MISMA terminal donde inicia Java o en la configuración de ejecución del IDE. `.env.example` es una referencia: Java no carga archivos `.env` automáticamente.

```powershell
$env:DB_USER='root'
$env:DB_PASSWORD='SU_CONTRASENA_LOCAL'
$env:DB_URL='jdbc:mysql://localhost:3306/sistemas_transaccionales_s6?serverTimezone=America/Bogota&useSSL=false&allowPublicKeyRetrieval=true'
$env:DB_SECOND_URL='jdbc:mysql://localhost:3306/sistemas_transaccionales_s6_destino?serverTimezone=America/Bogota&useSSL=false&allowPublicKeyRetrieval=true'
mvn -s .mvn/settings.xml clean compile exec:exec '-Ddemo.exercise=menu'
```

La configuración sin TLS es exclusivamente para la práctica local. Para servidores separados, use URLs con los hosts correspondientes y TLS verificado. El usuario necesita acceso a ambos esquemas. La recuperación XA en MySQL 8.0 puede necesitar el privilegio XA_RECOVER_ADMIN; no se requiere concederlo a una aplicación web normal.

No ejecute `exec:java`: Main termina la JVM de la demostración para cerrar hilos auxiliares de Storm. `exec:exec` abre una JVM separada y deja Maven intacto.

## NetBeans

1. File > Open Project, seleccione esta carpeta con `pom.xml`.
2. Configure JDK 17 o 21 y espere las dependencias.
3. Configure las variables DB_USER y DB_PASSWORD en el entorno de ejecución o abra NetBeans desde una terminal que las tenga definidas. Las URLs predeterminadas son locales y apuntan a los esquemas de semana 6.
4. Ejecute `Main.java` como aplicación Java. También puede crear una acción Maven `compile exec:exec -Ddemo.exercise=menu`.
5. Elija 1, 2, 3 o 4; 5 ejecuta todos y 0 sale.

## IntelliJ

Abra `pom.xml`, importe Maven y ejecute `co.edu.uniminuto.transacciones.Main`. Configure variables en Run > Edit Configurations > Environment variables. Use un directorio de trabajo igual a la raíz del proyecto. Verifique que DB_URL y DB_SECOND_URL apunten a los esquemas del taller.

## Ejercicios y comprobación

```powershell
mvn -s .mvn/settings.xml compile exec:exec '-Ddemo.exercise=messaging'
mvn -s .mvn/settings.xml compile exec:exec '-Ddemo.exercise=cache'
mvn -s .mvn/settings.xml compile exec:exec '-Ddemo.exercise=storm'
mvn -s .mvn/settings.xml compile exec:exec '-Ddemo.exercise=2pc'
mvn -s .mvn/settings.xml compile exec:exec '-Ddemo.exercise=recover'
```

1. ActiveMQ: cola `pedidos.confirmados`, envío persistente, rollback del consumidor, reentrega y registro idempotente en MySQL. El commit MySQL y el commit JMS son separados; la clave eventId protege ante una reentrega posterior al commit SQL. No se afirma atomicidad global de estos dos commits.
2. Caché: JCache y dos miembros Hazelcast TCP 127.0.0.1:5706/5707. MISS en A carga MySQL; HIT en B comparte stock 10; invalidación en A se observa en B. TTL 60 segundos. Los dos miembros reales viven en una sola JVM: no se ha probado caída de host ni alta disponibilidad. La caché no reemplaza las validaciones transaccionales de inventario.
3. Storm: spout genera 150, 2500 y -10; bolt clasifica APROBADA, ALERTA y RECHAZADA; bolt JDBC guarda con commit y ack posterior. La clave primaria permite repetir la entrega sin duplicar el evento. -10 se guarda como evidencia de rechazo, no como débito aplicado a cuentas. El spout es finito y su cola pendiente está en memoria: no es una fuente durable para recuperación de procesos.
4. 2PC: dos conexiones y ramas XA. PREPARE de ambos recursos, diario forzado a disco, COMMIT. Rechazo controlado después del primer PREPARE provoca ROLLBACK. Las bases están en un servidor local; configure DB_SECOND_URL con otro servidor para separar físicamente los recursos. No borre `data/coordinador` mientras haya transacciones pendientes. Tras un COMMIT decidido, `recover` completa esa decisión y nunca intenta revertir la rama ya confirmada. XAER_NOTA en recuperación significa rama ya resuelta o ausente. No ejecute dos coordinadores al mismo tiempo sobre el mismo diario. No se simuló una caída eléctrica ni partición de red.

## Consultas de verificación

```sql
SELECT * FROM sistemas_transaccionales_s6.cuentas;
SELECT * FROM sistemas_transaccionales_s6_destino.cuentas;
SELECT * FROM sistemas_transaccionales_s6.eventos;
SELECT * FROM sistemas_transaccionales_s6.transacciones_tiempo_real;
XA RECOVER;
```

## Archivos

- `MessagingDemo.java`, `CacheDemo.java`, `RealtimeDemo.java`, `TwoPhaseCommitDemo.java`: ejercicios 1 a 4.
- `DbConfig.java`: configuración y consultas JDBC.
- `Main.java`: menú y selección por argumentos.
- `sql/backup_inicial_s6.sql`: estructura y datos iniciales reproducibles.
- `sql/backup_resultados_s6.sql`: respaldo real posterior a pruebas, incluido solo en el paquete local.
- `docs/`: capturas de los registros reales y diagramas del informe, incluidos solo en el paquete local.

Las evidencias muestran ejecución mediante Java y Maven. Las capturas de registros se presentan en una vista HTML y no se hacen pasar por capturas del IDE. Si el docente exige pantallas específicas de NetBeans, ejecute los cuatro ejercicios allí y capture la salida y las consultas en Workbench.

Repositorio público: https://github.com/carlosapalpo/sistemas-transaccionales-s6-uniminuto

El repositorio incluye el código completo, las instrucciones y el script SQL con estructura y datos ficticios de ejemplo. El respaldo posterior a las pruebas y las evidencias se conservan únicamente en el ZIP local del taller. El código público puede descargarse con Code > Download ZIP o clonarse con `git clone https://github.com/carlosapalpo/sistemas-transaccionales-s6-uniminuto.git`.
