# Documentación Técnica

Esta sección describe la **arquitectura**, **componentes**, **modelo de datos (SQLite)** y las **decisiones de diseño** de los scripts `scan_file_anh_lotes_reiniciar.py` y `scan_file_anh_lotes_reiniciar_md5.py`. Ambos programas comparten el mismo flujo general y la misma base de reanudación, con la diferencia de que la versión MD5 añade el cálculo de huella digital y la columna `error_file`. Este documento sirve como referencia para desarrolladores y mantenedores.

## 1. Arquitectura general

El objetivo arquitectónico es recorrer recursivamente un árbol de directorios en una ruta UNC, inventariar los metadatos de cada archivo, clasificar los PDFs (¿contienen texto o son solo imágenes?) y generar una salida en CSV. Para permitir pausas y reanudaciones, se persiste el progreso en una base de datos SQLite. La versión MD5 además calcula una huella digital para detectar duplicados y registra cualquier error por archivo.

En alto nivel, el flujo es el siguiente:

```mermaid
flowchart TD
    A[Inicio CLI (argparse)] --> B[Inicialización]
    B --> B1[Logger rotatorio (scan_errores.log)]
    B --> B2[BD SQLite (processed_files, scan_progress)]
    B --> B3[Lectura de flags (--root, --scan-mode, etc.)]

    B --> C[Walker de archivos (os.walk + normalize_path)]
    C -->|Cada archivo| D[os.stat con reintento]
    D -->|Error| E1[Fila de error (stat)]
    D -->|OK| E2[Filtros de extensión y exclusión]
    E2 --> F[Metadatos (tamaño, extensión, nombre)]
    F --> G1[MD5 en streaming]
    F --> G2{¿Es PDF?}
    G1 --> G2
    G2 -->|Sí| H[Clasificación PDF]
    G2 -->|No| I[Escritura directa en CSV]
    H --> J[Combinar errores → error_file]
    I --> J
    J --> K[Escribir fila en CSV (safe_writerow)]
    K --> L[Actualizar processed_files]
    L --> M[Commit periódico + GC]
    M --> C
    subgraph Resultados
        K --> R1[CSV(s)]
        B1 --> R2[scan_errores.log]
        B2 --> R3[scan_state.sqlite]
    end
```

### Diseño de reanudación

Para soportar corridas largas e interrupciones, los scripts usan una base de datos SQLite con dos tablas:

- `processed_files(path_abs TEXT PRIMARY KEY, size_bytes INTEGER, mtime_ns INTEGER, written_ts INTEGER)`: almacena la ruta absoluta en formato UNC extendido, el tamaño en bytes, la marca de tiempo de modificación (nanosegundos) y la fecha de escritura. Al volver a ejecutar el script, se consulta esta tabla para omitir archivos que no han cambiado de tamaño ni de fecha de modificación【958047787344951†L228-L241】.
- `scan_progress(topdir TEXT PRIMARY KEY, finished INTEGER, finished_ts INTEGER)`: en modo `per-topdir`, marca las carpetas de primer nivel que ya se han completado. Esto permite reanudar a partir de la siguiente carpeta en la lista predeterminada o en la lista pasada por `--topdirs`.

La elección de SQLite obedece a su ligereza y portabilidad. Cada vez que se procesa un archivo correctamente, se ejecuta `INSERT OR REPLACE` sobre `processed_files`. Para minimizar el uso de memoria y evitar transacciones demasiado grandes, se realiza un commit periódico (controlado con `--progress-every`).

### Tratamiento de archivos y errores

1. **Lectura de metadatos**: mediante `os.stat` se obtienen tamaño y fechas. Si falla el primer intento, se espera un tiempo aleatorio corto y se reintenta; si vuelve a fallar, se registra una fila con los campos vacíos y `error_file` indicando el error.
2. **Filtrado**: se aplican listas de extensiones incluidas/excluidas (`--include-ext`, `--exclude-ext`), así como directorios excluidos (`--exclude-dirs`).
3. **MD5**: la versión MD5 calcula la huella en streaming (bloques de 8 MB por defecto) con `hashlib.md5()`. Si se produce un fallo de lectura, se deja la columna MD5 vacía y se anota un mensaje en `error_file` (prefijo `md5:`)【322†source】.
4. **Clasificación de PDFs**: se utiliza PyMuPDF (`fitz`) para abrir el PDF y se extrae texto de las primeras páginas (configurable mediante `--pdf-pages`). Si alguna contiene texto, se asigna `PDF_imagen=0`; de lo contrario, `PDF_imagen=1`. Los PDFs encriptados o dañados generan `PDF_imagen=""` y un mensaje de error. Esta operación puede ejecutarse en paralelo mediante `ThreadPoolExecutor` para mejorar el rendimiento en lotes grandes【322†source】.
5. **Escritura robusta en CSV**: `safe_writerow()` encapsula la escritura y utiliza reintentos con retraso exponencial si se produce un `PermissionError`, típico cuando el archivo CSV está abierto en otra aplicación. Cada fila del CSV se construye con `make_row()` en el orden definido en el encabezado (véase la documentación de usuario).
6. **Combinación de errores**: las excepciones de MD5, PDF o cualquier otra operación se concatenan en la columna `error_file`. Esto garantiza que, incluso con fallos, cada archivo genera una fila con información sobre el problema encontrado.

### Procesamiento en paralelo

El módulo `concurrent.futures` se emplea para crear un `ThreadPoolExecutor` que procesa la clasificación de PDF en segundo plano. El hilo principal continúa recorriendo archivos y calculando MD5, mientras que los hilos consumidores actualizan los contadores de PDFs (`pdf_1`, `pdf_0`, `pdf_x`) y escriben filas al CSV cuando terminan. De este modo, se optimiza el uso de CPU en sistemas multinúcleo sin complicar la lógica principal.

### Control de memoria

En escaneos de miles de archivos, PyMuPDF puede acumular objetos en memoria. Por ello, tras procesar un número configurable de archivos (`--store-shrink`), se invoca `fitz.TOOLS.store_shrink()` para liberar memoria interna. Además, el recolector de basura de Python (`gc.collect()`) se ejecuta periódicamente (`--gc-every`) para contener el uso de RAM.

## 2. Componentes clave

### `normalize_path()`

Convierte rutas locales y UNC en formato extendido (`\\?\UNC\...`) y elimina caracteres inválidos. Permite superar la limitación de 260 caracteres de Windows. Si una ruta no es válida (por ejemplo, termina en espacio o punto), devuelve `None`.

### `file_md5()` (versión MD5)

Calcula el hash MD5 de un archivo leyendo por bloques. El bloque por defecto es de 8 MB, pero puede ajustarse modificando el código. Devuelve una cadena hexadecimal de 32 caracteres. En caso de fallo de lectura, lanza una excepción que se captura en la lógica principal.

### `classify_pdf_with_error()`

Abre un PDF con PyMuPDF. Si está encriptado o se produce cualquier error, lo registra en el log y devuelve `("", mensaje)`. Para PDFs legibles, lee hasta `max_pages` páginas y comprueba si `get_text()` devuelve contenido no vacío. Si se detecta texto, se asigna `"0"`; si no, `"1"`.

### `safe_writerow()`

Escribe una fila en el CSV con reintentos. Envuelve el método `csv.writer.writerow()` y, ante errores de escritura, espera un tiempo incremental antes de reintentar. Esto soluciona errores temporales (p. ej. archivos bloqueados por aplicaciones externas). Lanza la excepción final si no puede completar la escritura.

### `processed_files` y `scan_progress`

Estas tablas de SQLite se gestionan mediante funciones auxiliares (`load_state()`, `upsert_state()`) que insertan o actualizan filas. `upsert_state()` usa `INSERT OR REPLACE` y actualiza `written_ts` con la fecha actual. Las consultas para omitir archivos usan `path_abs` como clave primaria. Los campos `size_bytes` y `mtime_ns` permiten saber si un archivo cambió de tamaño o fecha de modificación desde la última ejecución.

## 3. Modelo de datos

La base de datos SQLite (por defecto `scan_state.sqlite`) contiene dos tablas:

```sql
CREATE TABLE processed_files (
    path_abs    TEXT PRIMARY KEY,
    size_bytes  INTEGER,
    mtime_ns    INTEGER,
    written_ts  INTEGER
);

CREATE TABLE scan_progress (
    topdir      TEXT PRIMARY KEY,
    finished    INTEGER,
    finished_ts INTEGER
);
```

Al procesar un archivo por primera vez o detectarse un cambio, se inserta o reemplaza la fila correspondiente. En modo `per-topdir`, cuando se finaliza una subcarpeta, se actualiza `scan_progress` con `finished=1`.

## 4. Decisiones de diseño

- **Persistencia mínima pero suficiente**: SQLite facilita reanudar y evita que un error o apagado inesperado cause pérdidas de avance.  No se almacena toda la salida en la base (solo los metadatos de reanudación) para no duplicar la información que ya está en el CSV.
- **Separación de responsabilidades**: la lectura de metadatos, el cálculo de hashes y la clasificación PDF son funciones independientes, lo que facilita su mantenimiento y posible sustitución por otros algoritmos (por ejemplo, añadir SHA256).
- **Robustez frente a errores**: los scripts siguen el principio de “registrar y continuar”; cualquier fallo en un archivo se anota pero no detiene el proceso. La columna `error_file` ofrece trazabilidad por fila y el log central almacena el detalle para análisis posterior.
- **Configurabilidad**: casi todos los parámetros son argumentos de línea de comandos (modo de escaneo, número de hilos, filtros, límites de archivos, frecuencias de commit, etc.), para adaptarse a distintos entornos y volúmenes de datos.

## 5. Ampliaciones y extensibilidad

El diseño modular permite añadir nuevas columnas al CSV (por ejemplo, propietario del archivo, fecha de creación, hash SHA256) sin alterar la estructura básica. También se podrían agregar más clasificadores de tipos de archivos (documentos ofimáticos, imágenes) siguiendo el patrón de la clasificación PDF. La reanudación basada en la combinación de ruta, tamaño y mtime se podría enriquecer para detectar archivos renombrados.

## 6. Conclusión

Los scripts `scan_file_anh_lotes_reiniciar.py` y `scan_file_anh_lotes_reiniciar_md5.py` proporcionan una solución robusta y configurable para inventariar grandes repositorios de archivos en entornos Windows. La combinación de un caminador de archivos, reanudación con SQLite, clasificación de PDFs y cálculo de MD5 permite obtener un inventario técnico de gran detalle. La persistencia de errores y la trazabilidad facilitan la depuración y la migración de datos al BIEN. Gracias a su arquitectura modular, el código puede adaptarse a nuevas necesidades y crecer con los requisitos de la Agencia Nacional de Hidrocarburos.