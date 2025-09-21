#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
https://chatgpt.com/g/g-p-6888c8776d7c819191244a67af297365/c/68bf3e82-f9b4-8325-9534-9c1708dc6d37
Escáner NTFS sobre ruta UNC (Windows 11) con CSV en streaming, reanudación
(SQLite), detección de PDF imagen/texto con PyMuPDF y capacidad de
procesamiento por subcarpetas de primer nivel.  Esta versión incorpora
medidas de gestión de memoria para ejecuciones largas y corrige un error
de uso de variables globales en versiones previas.

Características principales:
  - Dos modos de escaneo: ``all`` procesa toda la estructura de directorios
    en un solo CSV; ``per-topdir`` (por defecto) genera un CSV separado
    por cada subcarpeta de primer nivel, preservando la reanudación.
  - Reanudación robusta mediante SQLite: los archivos ya procesados se
    registran con su ruta, tamaño y marca de modificación para evitar
    reprocesos tras un reinicio.  Además, se lleva un control de
    ``topdirs`` terminados para saltarlos en reinicios.
  - Detección de PDFs con PyMuPDF: cada PDF se analiza para determinar si
    contiene sólo imágenes (``"1"``), texto (``"0"``) o es indeterminado
    (``""``).  Los errores al abrir o analizar PDFs se registran y
    permiten continuar.
  - Control de memoria: se invoca ``gc.collect()`` y ``fitz.TOOLS.store_shrink()``
    periódicamente (configurable con ``--gc-every`` y ``--store-shrink``)
    para liberar cachés internos y reducir el uso de memoria en procesados
    masivos.  Se cierra cada documento PDF en un bloque ``finally`` y se
    aplica ``store_shrink`` después de cada clasificación.
  - Reanudación configurable: se puede reiniciar completamente (``--reset-state``)
    o ignorar el estado anterior (``--fresh``).  También se pueden
    seleccionar únicamente algunas subcarpetas (``--topdirs``) o
    procesarlas todas en el orden prefijado.
  - Manejo robusto de errores de E/S y PyMuPDF: cualquier excepción se
    captura, se registra en un log rotatorio y se continúa con el
    siguiente archivo.

Uso recomendado desde consola (Anaconda Prompt):

  python "C:\ANH\ANH\scan_file_anh.py" \\
      --scan-mode per-topdir \\
      --workers 12 \\
      --pdf-pages 1 \\
      --progress-every 500 \\
      --gc-every 5000 \\
      --store-shrink 50

Por defecto, el script procesa las 18 subcarpetas de primer nivel
especificadas en ``TOPDIRS_DEFAULT``.  Si alguna no existe, se informa
y se continúa con las siguientes.  Con ``--topdirs`` se puede limitar a
un subconjunto específico (sin alterar la ordenación predeterminada).
"""

import os
import csv
import time
import logging
from logging.handlers import RotatingFileHandler
import sqlite3
import argparse
import concurrent.futures
import random
import gc
import errno
from typing import Optional, Tuple, List, Dict, Iterable

# Importación opcional de PyMuPDF.  Si no está disponible, las
# clasificaciones de PDF se marcarán como indeterminadas.
try:
    import fitz  # type: ignore
except Exception:
    fitz = None

# ----------------------------------------------------------------------
# Constantes de configuración
# ----------------------------------------------------------------------

# Rutas por defecto (usar prefijos UNC extendidos; sin barra final en ROOT)
DEFAULT_ROOT: str = r"\\?\UNC\gg.anh.gov.co\RepositorioGG"
DEFAULT_OUT: str = r"C:\ANH\ANH\salida_scanar_full.csv"
DEFAULT_STATE: str = r"C:\ANH\ANH\scan_state.sqlite"
DEFAULT_LOG: str = r"C:\ANH\ANH\scan_errores.log"

# Otros ajustes por defecto
DEFAULT_PDF_PAGES: int = 5
DEFAULT_PROGRESS_EVERY: int = 500
DEFAULT_WORKERS: int = min(8, max(1, (os.cpu_count() or 1) * 2))

# Ajustes de gestión de memoria.  ``DEFAULT_GC_EVERY`` controla cada cuántos
# archivos procesados se fuerza una recolección de basura y se encoge el
# almacén de PyMuPDF.  ``DEFAULT_STORE_SHRINK`` especifica cuántas veces
# se invoca ``fitz.TOOLS.store_shrink()`` en cada limpieza (los valores
# altos comprimen más memoria pero consumen más CPU).
DEFAULT_GC_EVERY: int = 5000
DEFAULT_STORE_SHRINK: int = 50

# Lista de subcarpetas de primer nivel por defecto (orden obligatorio).
TOPDIRS_DEFAULT: List[str] = [
    "1_INFORMACION_QC",
    "2_DESCARGAS",
    "AppsPortables",
    "Contratos VT",
    "doris_manriqueb",
    "EPIS Manual 2021",
    "EPIS_VCH",
    "EPIS_VORP",
    "gladys-morantesp",
    "HIDROGEOLOGIA",
    "ID 1686535 ENTREGA PROD RECUR PROSP 10-12-2024",
    "INSUMOS_BD_PROYECTOS_VT",
    "laurent.ramos",
    "Manual 2021",
    "Mario.Meza V2",
    "TEMPLATE_PETREL",
    "Temp_DanielaC",
    "UPRA_ID_1650946",
]

# Prefijos extendidos para normalización de rutas
_EXTENDED_PREFIX: str = "\\\\?\\"
_EXTENDED_UNC: str = "\\\\?\\UNC\\"

# Conjunto de nombres reservados en Windows que no se pueden usar como
# nombres de archivo.  Se utiliza para sanitizar los nombres de CSV por
# carpeta.
_RESERVED_NAMES: set = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}

# Caracteres ilegales en nombres de archivo
_ILLEGAL_CHARS: str = '<>:"/\\|?*'

# ----------------------------------------------------------------------
# Utilidades de manejo de rutas y normalización
# ----------------------------------------------------------------------

def _collapse_backslashes(rest: str) -> str:
    """Colapsa '\\' duplicados en la parte posterior a un prefijo extendido."""
    while "\\\\" in rest:
        rest = rest.replace("\\\\", "\\")
    return rest


def _strip_trailing(rest: str) -> str:
    """Elimina barras finales redundantes excepto en UNC raiz."""
    return rest.rstrip("\\")


def _has_bad_component(parts: List[str]) -> bool:
    """Comprueba si algún componente es vacío o termina en espacio/punto."""
    for p in parts:
        if p == "" or p.endswith(" ") or p.endswith("."):
            return True
    return False


def normalize_path(path: str) -> Optional[str]:
    """
    Normaliza una ruta al formato extendido de Windows.  Devuelve ``None`` si
    se detectan componentes inválidos.  Maneja rutas locales y UNC.
    """
    p = path.rstrip("\\")
    # Ya extendida
    if p.startswith(_EXTENDED_PREFIX):
        if p.startswith(_EXTENDED_UNC):
            rest = p[len(_EXTENDED_UNC):]
            rest = _collapse_backslashes(rest)
            parts = rest.split("\\")
            if _has_bad_component(parts):
                return None
            if len(parts) > 2:
                rest = _strip_trailing(rest)
            return _EXTENDED_UNC + rest
        else:
            rest = p[len(_EXTENDED_PREFIX):]
            rest = _collapse_backslashes(rest)
            parts = rest.split("\\")
            if _has_bad_component(parts):
                return None
            rest = _strip_trailing(rest)
            return _EXTENDED_PREFIX + rest
    # UNC estándar
    if p.startswith("\\\\"):
        rest = p[2:]
        rest = _collapse_backslashes(rest)
        parts = rest.split("\\")
        if len(parts) < 2 or _has_bad_component(parts):
            return None
        if len(parts) > 2:
            rest = _strip_trailing(rest)
        return _EXTENDED_UNC + rest
    # Ruta local
    abs_path = os.path.abspath(p)
    rest = _collapse_backslashes(abs_path)
    parts = rest.split("\\")
    if _has_bad_component(parts):
        return None
    rest = _strip_trailing(rest)
    return _EXTENDED_PREFIX + rest


def init_logger(log_path: str) -> logging.Logger:
    """Inicializa un logger rotatorio para capturar errores y mensajes."""
    logger = logging.getLogger("scan_ntfs")
    # Evita agregar múltiples handlers si el logger ya se inicializó
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    dirpath = os.path.dirname(log_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024,
                                  backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def init_sqlite_state(db_path: str, reset: bool = False) -> sqlite3.Connection:
    """
    Crea o abre la base de datos de estado.  Si ``reset`` es True, se
    elimina la base existente antes de crearla.  Esta BD almacena los
    archivos procesados y el progreso por carpeta para permitir reanudación.
    """
    if reset and os.path.exists(db_path):
        os.remove(db_path)
    dirpath = os.path.dirname(db_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    conn = sqlite3.connect(db_path)
    # Tabla de archivos procesados: path_abs clave primaria
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files(
            path_abs   TEXT PRIMARY KEY,
            size_bytes INTEGER NOT NULL,
            mtime_ns   INTEGER NOT NULL,
            written_ts INTEGER NOT NULL
        )
        """
    )
    # Tabla de progreso por topdir: finished=1 cuando se termina la carpeta
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_progress(
            topdir      TEXT PRIMARY KEY,
            finished    INTEGER NOT NULL DEFAULT 0,
            finished_ts INTEGER
        )
        """
    )
    conn.commit()
    return conn


def mark_topdir_finished(conn: sqlite3.Connection, topdir: str) -> None:
    """Marca una subcarpeta como finalizada en la tabla scan_progress."""
    conn.execute(
        "INSERT INTO scan_progress(topdir,finished,finished_ts) VALUES(?,?,?) "
        "ON CONFLICT(topdir) DO UPDATE SET finished=excluded.finished, finished_ts=excluded.finished_ts",
        (topdir, 1, int(time.time()))
    )
    conn.commit()


def is_topdir_finished(conn: sqlite3.Connection, topdir: str) -> bool:
    """Devuelve True si la subcarpeta ya fue finalizada (finished=1)."""
    cur = conn.cursor()
    cur.execute("SELECT finished FROM scan_progress WHERE topdir = ?", (topdir,))
    row = cur.fetchone()
    return bool(row and row[0] == 1)


def reset_topdir_progress(conn: sqlite3.Connection, topdir: str) -> None:
    """Elimina el registro de progreso de una subcarpeta (para reprocesarla)."""
    conn.execute("DELETE FROM scan_progress WHERE topdir = ?", (topdir,))
    conn.commit()


def already_processed(conn: sqlite3.Connection, path_abs: str,
                      size_bytes: int, mtime_ns: int) -> bool:
    """
    Consulta la BD para saber si un archivo (ruta, tamaño y mtime) ya se
    procesó.  Devuelve True si existe y coincide tamaño y fecha; False
    en otro caso.
    """
    cur = conn.cursor()
    cur.execute("SELECT size_bytes, mtime_ns FROM processed_files WHERE path_abs = ?",
                (path_abs,))
    row = cur.fetchone()
    return bool(row and row[0] == size_bytes and row[1] == mtime_ns)


def upsert_state(conn: sqlite3.Connection, path_abs: str,
                 size_bytes: int, mtime_ns: int) -> None:
    """
    Inserta o actualiza el registro de un archivo procesado en la base de
    datos.  Utiliza la ruta absoluta como clave primaria.
    """
    conn.execute(
        "INSERT OR REPLACE INTO processed_files(path_abs,size_bytes,mtime_ns,written_ts)"
        " VALUES (?,?,?,?)",
        (path_abs, size_bytes, mtime_ns, int(time.time() * 1000)),
    )


def classify_pdf(path_abs: str, max_pages: int = 5) -> str:
    """
    Clasifica un archivo PDF como:
      - ``"1"`` si sólo contiene imágenes (no se encuentra texto en las
        primeras ``max_pages`` páginas),
      - ``"0"`` si se detecta texto en las primeras ``max_pages`` páginas,
      - ``""`` si no se pudo determinar (errores, PDF vacío, no PDF, etc.).

    Se captura cualquier excepción de PyMuPDF o de I/O, se registra en
    el logger y se devuelve "".  Además, tras analizar un documento,
    se asegura el cierre de ``doc`` y se realiza ``store_shrink`` para
    liberar cachés internos de MuPDF.
    """
    if fitz is None:
        return ""
    logger = logging.getLogger("scan_ntfs")
    doc = None
    try:
        try:
            doc = fitz.open(path_abs)
        except Exception:
            # Reintento por posibles bloqueos temporales o errores transitorios
            time.sleep(random.uniform(0.2, 0.5))
            doc = fitz.open(path_abs)
        if doc.is_encrypted:
            logger.error(f"PDF encriptado: {path_abs}")
            return ""
        # Limitar a 'max_pages' o al total de páginas
        pages = min(max_pages, max(0, doc.page_count))
        for i in range(pages):
            try:
                page = doc.load_page(i)
                if page.get_text().strip():
                    return "0"
            except Exception as e:
                logger.error(f"Error leyendo página {i+1} de {path_abs}: {e!r}")
                return ""
        return "1"
    except Exception as e:
        logger.error(f"Error procesando PDF {path_abs}: {e!r}")
        return ""
    finally:
        # Asegurar cierre del documento
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass
        # Intentar liberar cachés de MuPDF (store_shrink)
        if fitz is not None and hasattr(fitz, "TOOLS"):
            try:
                fitz.TOOLS.store_shrink(DEFAULT_STORE_SHRINK)
            except Exception:
                pass


def bytes_to_kb_mb(size_bytes: int) -> Tuple[float, float]:
    """Convierte bytes a kilobytes y megabytes con precisión decimal."""
    return size_bytes / 1024.0, size_bytes / (1024.0 * 1024.0)


def list_first_level_dirs(root_path: str) -> List[str]:
    """Devuelve las subcarpetas inmediatas de ``root_path`` (primer nivel)."""
    try:
        entries = os.listdir(root_path)
    except Exception:
        return []
    dirs: List[str] = []
    for e in entries:
        p = os.path.join(root_path, e)
        try:
            if os.path.isdir(p):
                dirs.append(e)
        except Exception:
            continue
    return dirs


def resolve_topdirs(root_path: str, cli_topdirs: Optional[str]) -> List[str]:
    """
    Determina la lista de subcarpetas de primer nivel a procesar.

    - Si se proporciona ``cli_topdirs``, se usa esa lista textual (respetando
      el orden indicado) y no se filtra la existencia.  Es decir, si una
      carpeta no existe, se informará más adelante y se saltará.
    - Si **no** se proporciona ``cli_topdirs``, se devuelve siempre
      ``TOPDIRS_DEFAULT`` en el orden definido.  Aunque alguna carpeta
      no exista, se incluirá en el ciclo y se imprimirá un mensaje de
      advertencia cuando no se encuentre.
    """
    if cli_topdirs:
        return [t.strip() for t in cli_topdirs.split(",") if t.strip()]
    return list(TOPDIRS_DEFAULT)


def sanitize_for_filename(name: str) -> str:
    """
    Sanitiza una cadena para usarla como parte de un nombre de archivo en
    Windows: reemplaza caracteres ilegales, espacios por guiones bajos y
    evita nombres reservados añadiendo un sufijo.
    """
    s = name.strip().replace(" ", "_")
    for ch in _ILLEGAL_CHARS:
        s = s.replace(ch, "_")
    base = os.path.splitext(s)[0].upper()
    if base in _RESERVED_NAMES:
        s = s + "_dir"
    return s


def compute_out_csv(base_out: str, topdir: str) -> str:
    """
    Calcula la ruta del CSV de salida para una subcarpeta ``topdir``.
    - El directorio es el de ``base_out``.
    - El prefijo se obtiene del nombre base de ``base_out``.  Si empieza
      por ``salida_scanar``, se usa exactamente ese prefijo.  De lo
      contrario, se usa el nombre base completo (sin extensión).
    - El nombre resultante es ``<prefijo>_<TopDirSanitizado>.csv``.
    """
    out_dir = os.path.dirname(base_out) or "."
    base_name = os.path.basename(base_out)
    name_noext = os.path.splitext(base_name)[0]
    if name_noext.lower().startswith("salida_scanar"):
        prefix = "salida_scanar"
    else:
        prefix = name_noext
    safe_top = sanitize_for_filename(topdir)
    return os.path.join(out_dir, f"{prefix}_{safe_top}.csv")


# ----------------------------------------------------------------------
# Escritura segura de CSV
# ----------------------------------------------------------------------
def safe_writerow(csvw: csv.writer, row: List[str], csv_fp, log: logging.Logger, retries: int = 6) -> None:
    """
    Escribe una fila en el CSV con reintentos ante ``PermissionError`` u
    ``OSError`` relacionados con accesos concurrentes al archivo.  Si la
    escritura falla, realiza un flush del archivo y espera un tiempo
    incremental antes de reintentar.  Levanta ``PermissionError`` si no
    se consigue escribir tras varios intentos.

    :param csvw: escritor CSV (csv.writer)
    :param row: fila a escribir (lista de strings)
    :param csv_fp: archivo abierto (usado para flush)
    :param log: logger para registrar errores
    :param retries: número máximo de reintentos antes de fallar
    """
    for i in range(retries):
        try:
            csvw.writerow(row)
            if i > 0:
                # Si hubo reintentos, hacer flush explícito
                try:
                    csv_fp.flush()
                except Exception:
                    pass
            return
        except PermissionError as e:
            # Error típico de archivo bloqueado
            wait = 0.5 * (2 ** i)
            log.error(
                f"PermissionError escribiendo CSV (reintento {i+1}/{retries}, espera {wait:.1f}s): {e!r}"
            )
            try:
                csv_fp.flush()
            except Exception:
                pass
            time.sleep(wait)
        except OSError as e:
            # Manejar sharing violations u otras denegaciones de acceso
            winerr = getattr(e, "winerror", None)
            if winerr in (32, 33) or e.errno in (errno.EACCES, errno.ETXTBSY):
                wait = 0.5 * (2 ** i)
                log.error(
                    f"OSError escribiendo CSV (reintento {i+1}/{retries}, espera {wait:.1f}s): {e!r}"
                )
                try:
                    csv_fp.flush()
                except Exception:
                    pass
                time.sleep(wait)
            else:
                # Otros errores se propagan
                raise
    # Tras agotar reintentos, relanzar
    raise PermissionError("No se pudo escribir la fila CSV tras múltiples reintentos.")


def walk_files_under(root_path: str, exclude_dirs: Optional[List[str]] = None) -> Iterable[Tuple[str, str]]:
    """
    Generador que recorre recursivamente los archivos bajo ``root_path``.
    Devuelve tuplas (ruta_absoluta_normalizada, ruta_relativa).  Usa
    ``exclude_dirs`` para omitir subdirectorios por nombre literal.
    Si la ruta no se puede normalizar (componentes inválidos), devuelve
    ``(None, None)`` como marcador de error.
    """
    for cur_root, dirs, files in os.walk(root_path, topdown=True):
        # Filtrar directorios excluidos
        if exclude_dirs:
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
        # Omitir enlaces simbólicos
        dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(cur_root, d))]
        for fname in files:
            abs_p = os.path.join(cur_root, fname)
            p_abs = normalize_path(abs_p)
            if not p_abs:
                # Ruta inválida: se puede llevar conteo de errores externamente
                yield None, None
                continue
            try:
                rel_path = os.path.relpath(p_abs, root_path)
            except Exception:
                if p_abs.lower().startswith(root_path.lower()):
                    rel_path = p_abs[len(root_path):].lstrip("\\/")
                else:
                    rel_path = p_abs
            yield p_abs, rel_path


def main() -> None:
    """
    Función principal: parsea argumentos, inicializa estructuras y
    recorre los directorios.  Maneja tanto el modo global (``all``)
    como el modo por subcarpetas (``per-topdir``).  Implementa
    reanudación con SQLite y limpieza periódica de memoria.
    """
    # Declarar globales modificado para poder reasignar después de parsear argumentos
    global DEFAULT_GC_EVERY, DEFAULT_STORE_SHRINK

    parser = argparse.ArgumentParser(
        description=(
            "Escáner NTFS (UNC) → CSV con reanudación, clasificación de PDFs "
            "y control de memoria."
        )
    )
    parser.add_argument("--root", default=DEFAULT_ROOT,
                        help="Ruta raíz UNC (usar prefijo \\?\\UNC\\…).")
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help="CSV base de salida.  Su directorio y prefijo se usan para generar los CSV en modo per-topdir.")
    parser.add_argument("--state", default=DEFAULT_STATE, help="Ruta del archivo SQLite de estado.")
    parser.add_argument("--log", default=DEFAULT_LOG, help="Archivo de log rotatorio.")
    parser.add_argument("--pdf-pages", type=int, default=DEFAULT_PDF_PAGES,
                        help="Número de páginas a leer para clasificar PDFs.")
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY,
                        help="Cantidad de archivos procesados entre mensajes de progreso y commits.")
    parser.add_argument("--include-ext", help="Extensiones a incluir (separadas por coma, sin punto).")
    parser.add_argument("--exclude-ext", help="Extensiones a excluir (separadas por coma, sin punto).")
    parser.add_argument("--exclude-dirs", help="Directorios a excluir (separados por coma).")
    parser.add_argument("--limit", type=int,
                        help="Máximo de archivos a procesar (para pruebas; omite el resto).")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignora el estado y reprocesa todos los archivos.")
    parser.add_argument("--reset-state", action="store_true",
                        help="Elimina la BD de estado al iniciar.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help="Número de hilos para clasificación de PDFs (>=1).")
    # Flags del nuevo comportamiento
    parser.add_argument("--scan-mode", choices=["all", "per-topdir"], default="per-topdir",
                        help="Modo de escaneo: 'all' genera un único CSV, 'per-topdir' uno por subcarpeta.")
    parser.add_argument("--topdirs", help="Lista de subcarpetas de primer nivel (coma separadas) a procesar.")
    parser.add_argument("--add-topdir-col", action="store_true",
                        help="Agrega una columna extra 'top_level_dir' al CSV.")
    parser.add_argument("--rescan-finished", help="Lista de topdirs a reprocesar (borra su checkpoint).")
    # Ajustes de memoria
    parser.add_argument("--gc-every", type=int, default=DEFAULT_GC_EVERY,
                        help="Archivos entre llamadas a gc.collect() y store_shrink().")
    parser.add_argument("--store-shrink", type=int, default=DEFAULT_STORE_SHRINK,
                        help="Número de veces que se invoca fitz.TOOLS.store_shrink() en cada limpieza de memoria.")

    args = parser.parse_args()

    # Actualizar las constantes globales de GC según argumentos
    DEFAULT_GC_EVERY = args.gc_every
    DEFAULT_STORE_SHRINK = args.store_shrink

    # Inicializar logger
    log = init_logger(args.log)

    # Normalizar ruta raíz
    root_path = normalize_path(args.root)
    if not root_path:
        log.error(f"Ruta raíz inválida: {args.root}")
        print("No se puede acceder a la raíz.")
        return
    root_path = root_path.rstrip("\\")
    if not os.path.isdir(root_path):
        log.error(f"No se puede acceder a la raíz: {root_path}")
        print("No se puede acceder a la raíz.")
        return

    # Inicializar SQLite
    conn = init_sqlite_state(args.state, reset=args.reset_state)

    # Parsear listas de extensiones y directorios excluidos
    include_exts = [e.strip().lower().lstrip(".") for e in args.include_ext.split(",")] if args.include_ext else None
    exclude_exts = [e.strip().lower().lstrip(".") for e in args.exclude_ext.split(",")] if args.exclude_ext else None
    exclude_dirs = [d.strip() for d in args.exclude_dirs.split(",")] if args.exclude_dirs else None

    # Permitir borrar checkpoints de ciertas carpetas
    if args.rescan_finished:
        for t in [x.strip() for x in args.rescan_finished.split(",") if x.strip()]:
            reset_topdir_progress(conn, t)

    # Contadores globales
    processed: int = 0
    skipped: int = 0
    errors_count: int = 0
    pdf_1: int = 0
    pdf_0: int = 0
    pdf_x: int = 0
    start_time = time.time()

    # Hilos y cola de futuros
    use_threads = (args.workers or 1) > 1
    executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    pending: Dict[concurrent.futures.Future, Tuple] = {}

    # Función local para impresión y commits periódicos + GC
    def periodic_actions(csv_fp: Optional[csv.writer], local_processed: int) -> None:
        """
        Realiza acciones periódicas: imprime progreso y hace commit/flush cada
        ``args.progress_every`` archivos, y ejecuta recolección de basura +
        ``store_shrink`` cada ``args.gc_every`` archivos.
        """
        nonlocal processed, skipped, errors_count
        if args.progress_every and local_processed % args.progress_every == 0:
            print(
                f"Progreso: {processed} procesados | {skipped} omitidos | "
                f"{errors_count} errores | PDFs [1={pdf_1}, 0={pdf_0}, ''={pdf_x}]"
            )
            try:
                if csv_fp:
                    # writer no es file, necesitamos el archivo real para flush
                    pass
            except Exception:
                pass
            conn.commit()
        # Recolección de basura
        if args.gc_every > 0 and local_processed % args.gc_every == 0 and local_processed != 0:
            print("[GC] Liberación de memoria…")
            # Llamar store_shrink globalmente
            if fitz is not None and hasattr(fitz, "TOOLS"):
                try:
                    fitz.TOOLS.store_shrink(DEFAULT_STORE_SHRINK)
                except Exception:
                    pass
            # Forzar recolección de basura
            gc.collect()

    try:
        if use_threads:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)

        # ------------------------------------------------------------------
        # Modo ALL: un único CSV para todo el recorrido
        # ------------------------------------------------------------------
        if args.scan_mode == "all":
            # Preparar CSV único
            out_mode = "w" if (args.fresh or args.reset_state or not os.path.exists(args.out)) else "a"
            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            csv_fp = open(args.out, out_mode, newline="", encoding="utf-8")
            csvw = csv.writer(csv_fp)
            base_header = [
                "nombre del archivo", "extensión del archivo",
                "tamaño del archivo en Kbytes", "tamaño del archivo en MBytes",
                "ruta relativa", "PDF_imagen"
            ]
            header = base_header + (["top_level_dir"] if args.add_topdir_col else [])
            if out_mode == "w":
                # Escribir encabezado con manejo de permisos
                safe_writerow(csvw, header, csv_fp, log)
                csv_fp.flush()

            # Handler para futuros PDF en modo 'all'
            def handle_pdf_future_all(fut: concurrent.futures.Future, rec: Tuple) -> None:
                nonlocal processed, pdf_1, pdf_0, pdf_x, errors_count
                (p_abs, name, ext, kb, mb, rel_path, st_size, st_mtime_ns, topdir_label) = rec
                try:
                    flag = fut.result()
                except Exception as e:
                    logging.getLogger("scan_ntfs").error(f"Error en worker PDF {p_abs}: {e!r}")
                    flag = ""
                # Actualizar contadores por tipo
                if flag == "1":
                    pdf_1 += 1
                elif flag == "0":
                    pdf_0 += 1
                else:
                    pdf_x += 1
                # Escribir fila CSV
                row: List[str] = [name, ext, f"{kb:.2f}", f"{mb:.2f}", rel_path, flag]
                if args.add_topdir_col:
                    row.append(topdir_label)
                safe_writerow(csvw, row, csv_fp, log)
                processed += 1
                try:
                    upsert_state(conn, p_abs, st_size, st_mtime_ns)
                except Exception as e:
                    errors_count += 1
                    logging.getLogger("scan_ntfs").error(f"Fallo upsert_state {p_abs}: {e!r}")
                periodic_actions(None, processed)

            # Recorrido recursivo
            for abs_path, rel_path in walk_files_under(root_path, exclude_dirs):
                if abs_path is None:
                    errors_count += 1
                    periodic_actions(None, processed)
                    continue
                # Obtener stat con reintentos
                try:
                    st = os.stat(abs_path)
                except Exception as e:
                    log.error(f"Error accediendo a {abs_path}: {e!r}")
                    time.sleep(random.uniform(0.2, 0.5))
                    try:
                        st = os.stat(abs_path)
                    except Exception as e2:
                        log.error(f"Fallo definitivo accediendo a {abs_path}: {e2!r}")
                        errors_count += 1
                        periodic_actions(None, processed)
                        continue
                # Saltar si ya está procesado (salvo --fresh)
                if not args.fresh and already_processed(conn, abs_path, st.st_size, st.st_mtime_ns):
                    skipped += 1
                    periodic_actions(None, processed)
                    continue
                # Filtrar por extensión
                name = os.path.basename(abs_path)
                name_noext, ext = os.path.splitext(name)
                ext = ext[1:].lower() if ext else ""
                if include_exts and ext not in include_exts:
                    periodic_actions(None, processed)
                    continue
                if exclude_exts and ext in exclude_exts:
                    periodic_actions(None, processed)
                    continue
                if args.limit and processed >= args.limit:
                    break
                kb, mb = bytes_to_kb_mb(st.st_size)
                # Clasificación PDF
                if ext == "pdf":
                    if use_threads:
                        fut = executor.submit(classify_pdf, abs_path, args.pdf_pages)
                        pending[fut] = (abs_path, name_noext, ext, kb, mb, rel_path, st.st_size, st.st_mtime_ns, "")
                        # Drenar futuros si se acumulan
                        if len(pending) >= (args.workers or 1) * 5:
                            done, _ = concurrent.futures.wait(list(pending.keys()), return_when=concurrent.futures.FIRST_COMPLETED)
                            for f in done:
                                rec = pending.pop(f)
                                handle_pdf_future_all(f, rec)
                            csv_fp.flush(); conn.commit()
                    else:
                        flag = classify_pdf(abs_path, args.pdf_pages)
                        if flag == "1":
                            pdf_1 += 1
                        elif flag == "0":
                            pdf_0 += 1
                        else:
                            pdf_x += 1
                        row = [name_noext, ext, f"{kb:.2f}", f"{mb:.2f}", rel_path, flag]
                        if args.add_topdir_col:
                            row.append("")
                        safe_writerow(csvw, row, csv_fp, log)
                        processed += 1
                        upsert_state(conn, abs_path, st.st_size, st.st_mtime_ns)
                        periodic_actions(None, processed)
                else:
                    row = [name_noext, ext, f"{kb:.2f}", f"{mb:.2f}", rel_path, ""]
                    if args.add_topdir_col:
                        row.append("")
                    safe_writerow(csvw, row, csv_fp, log)
                    processed += 1
                    upsert_state(conn, abs_path, st.st_size, st.st_mtime_ns)
                    periodic_actions(None, processed)
            # Drenar futuros restantes en modo 'all'
            if use_threads and pending:
                for f in concurrent.futures.as_completed(list(pending.keys())):
                    rec = pending.pop(f)
                    handle_pdf_future_all(f, rec)
            # Flush final y commit
            try:
                csv_fp.flush()
            except Exception:
                pass
            conn.commit()
            try:
                csv_fp.close()
            except Exception:
                pass

        # ------------------------------------------------------------------
        # Modo PER-TOPDIR: CSV separado por cada subcarpeta de primer nivel
        # ------------------------------------------------------------------
        else:
            topdirs = resolve_topdirs(root_path, args.topdirs)
            if not topdirs:
                print("No se encontraron subcarpetas de primer nivel para procesar.")
                return
            print(f"Topdirs a procesar (orden): {topdirs}")
            for topdir in topdirs:
                # Saltar si ya se marcó como terminado y no pedimos fresh
                if not args.fresh and is_topdir_finished(conn, topdir):
                    print(f"[{topdir}] ya finalizado anteriormente. Saltando…")
                    continue
                # Verificar existencia
                topdir_root = os.path.join(root_path, topdir)
                if not os.path.isdir(topdir_root):
                    print(f"[{topdir}] no existe o no es directorio. Saltando…")
                    continue
                # Determinar CSV por topdir
                out_csv = compute_out_csv(args.out, topdir)
                os.makedirs(os.path.dirname(out_csv), exist_ok=True)
                out_mode = "w" if (args.fresh or args.reset_state or not os.path.exists(out_csv)) else "a"
                csv_file = open(out_csv, out_mode, newline="", encoding="utf-8")
                csvw = csv.writer(csv_file)
                base_header = [
                    "nombre del archivo", "extensión del archivo",
                    "tamaño del archivo en Kbytes", "tamaño del archivo en MBytes",
                    "ruta relativa", "PDF_imagen"
                ]
                header = base_header + (["top_level_dir"] if args.add_topdir_col else [])
                if out_mode == "w":
                    # Escribir encabezado con manejo de permisos
                    safe_writerow(csvw, header, csv_file, log)
                    csv_file.flush()
                # Contadores por topdir
                td_processed = 0
                td_skipped = 0
                td_errors = 0
                td_pdf1 = 0
                td_pdf0 = 0
                td_pdfx = 0
                td_start = time.time()
                print(f"== Iniciando topdir: {topdir} ==")
                # Handler para futuros en este topdir
                def handle_pdf_future_td(fut: concurrent.futures.Future, rec: Tuple) -> None:
                    nonlocal processed, pdf_1, pdf_0, pdf_x, errors_count
                    # Declarar también td_errors y td_skipped para modificarlos dentro del closure
                    nonlocal td_processed, td_pdf1, td_pdf0, td_pdfx, td_errors, td_skipped
                    (p_abs, name, ext_, kb, mb, rel_path, st_size, st_mtime_ns, topdir_label) = rec
                    try:
                        flag = fut.result()
                    except Exception as e:
                        logging.getLogger("scan_ntfs").error(f"Error en worker PDF {p_abs}: {e!r}")
                        flag = ""
                    if flag == "1":
                        pdf_1 += 1; td_pdf1 += 1
                    elif flag == "0":
                        pdf_0 += 1; td_pdf0 += 1
                    else:
                        pdf_x += 1; td_pdfx += 1
                    row2: List[str] = [name, ext_, f"{kb:.2f}", f"{mb:.2f}", rel_path, flag]
                    if args.add_topdir_col:
                        row2.append(topdir_label)
                    safe_writerow(csvw, row2, csv_file, log)
                    processed += 1; td_processed += 1
                    try:
                        upsert_state(conn, p_abs, st_size, st_mtime_ns)
                    except Exception as e:
                        errors_count += 1; td_errors += 1
                        logging.getLogger("scan_ntfs").error(f"Fallo upsert_state {p_abs}: {e!r}")
                    periodic_actions(None, processed)
                # Recorrido de archivos del topdir
                for abs_path, rel_path in walk_files_under(topdir_root, exclude_dirs):
                    if abs_path is None:
                        errors_count += 1; td_errors += 1
                        periodic_actions(None, processed)
                        continue
                    # stat con reintentos
                    try:
                        st = os.stat(abs_path)
                    except Exception as e:
                        log.error(f"[{topdir}] Error accediendo a {abs_path}: {e!r}")
                        time.sleep(random.uniform(0.2, 0.5))
                        try:
                            st = os.stat(abs_path)
                        except Exception as e2:
                            log.error(f"[{topdir}] Fallo definitivo accediendo a {abs_path}: {e2!r}")
                            errors_count += 1; td_errors += 1
                            periodic_actions(None, processed)
                            continue
                    # Omitir si ya procesado (salvo fresh)
                    if not args.fresh and already_processed(conn, abs_path, st.st_size, st.st_mtime_ns):
                        skipped += 1; td_skipped += 1
                        periodic_actions(None, processed)
                        continue
                    fname = os.path.basename(abs_path)
                    name_noext, ext = os.path.splitext(fname)
                    ext = ext[1:].lower() if ext else ""
                    if include_exts and ext not in include_exts:
                        periodic_actions(None, processed)
                        continue
                    if exclude_exts and ext in exclude_exts:
                        periodic_actions(None, processed)
                        continue
                    if args.limit and processed >= args.limit:
                        break
                    kb, mb = bytes_to_kb_mb(st.st_size)
                    if ext == "pdf":
                        if use_threads:
                            fut = executor.submit(classify_pdf, abs_path, args.pdf_pages)
                            pending[fut] = (abs_path, name_noext, ext, kb, mb, rel_path, st.st_size, st.st_mtime_ns, topdir)
                            if len(pending) >= (args.workers or 1) * 5:
                                done, _ = concurrent.futures.wait(list(pending.keys()), return_when=concurrent.futures.FIRST_COMPLETED)
                                for f in done:
                                    rec = pending.pop(f)
                                    handle_pdf_future_td(f, rec)
                                csv_file.flush(); conn.commit()
                        else:
                            flag = classify_pdf(abs_path, args.pdf_pages)
                            if flag == "1":
                                pdf_1 += 1; td_pdf1 += 1
                            elif flag == "0":
                                pdf_0 += 1; td_pdf0 += 1
                            else:
                                pdf_x += 1; td_pdfx += 1
                            row3 = [name_noext, ext, f"{kb:.2f}", f"{mb:.2f}", rel_path, flag]
                            if args.add_topdir_col:
                                row3.append(topdir)
                            safe_writerow(csvw, row3, csv_file, log)
                            processed += 1; td_processed += 1
                            upsert_state(conn, abs_path, st.st_size, st.st_mtime_ns)
                            periodic_actions(None, processed)
                    else:
                        row3 = [name_noext, ext, f"{kb:.2f}", f"{mb:.2f}", rel_path, ""]
                        if args.add_topdir_col:
                            row3.append(topdir)
                        safe_writerow(csvw, row3, csv_file, log)
                        processed += 1; td_processed += 1
                        upsert_state(conn, abs_path, st.st_size, st.st_mtime_ns)
                        periodic_actions(None, processed)
                # Drenar futuros al finalizar subcarpeta
                if use_threads and pending:
                    for f in concurrent.futures.as_completed(list(pending.keys())):
                        rec = pending.pop(f)
                        handle_pdf_future_td(f, rec)
                try:
                    csv_file.flush()
                except Exception:
                    pass
                conn.commit()
                # Marcar subcarpeta como finalizada
                mark_topdir_finished(conn, topdir)
                # Resumen por topdir
                td_elapsed = time.time() - td_start
                td_rate = ((td_processed + td_skipped + td_errors) / td_elapsed) if td_elapsed > 0 else 0.0
                print(
                    f"[{topdir}] Resumen → procesados:{td_processed} omitidos:{td_skipped} "
                    f"errores:{td_errors} | PDFs[1={td_pdf1},0={td_pdf0},''={td_pdfx}] "
                    f"| tasa:{td_rate:.1f} arch/s"
                )
                try:
                    csv_file.close()
                except Exception:
                    pass
        # Fin else modo per-topdir

        # Drenaje final de futuros pendientes
        if use_threads and pending:
            for f in concurrent.futures.as_completed(list(pending.keys())):
                try:
                    _ = f.result()
                except Exception:
                    pass
    finally:
        # Cierre ordenado de recursos comunes
        try:
            if executor is not None:
                executor.shutdown(wait=True)
        except Exception:
            pass
        try:
            conn.commit()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        # Tiempo total y resumen global
        elapsed = time.time() - start_time
        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)
        s = int(elapsed % 60)
        dur = (f"{h}h " if h else "") + (f"{m}m " if (h or m) else "") + f"{s}s"
        total = processed + skipped + errors_count
        rate = (total / elapsed) if elapsed > 0 else 0.0
        print("============================================================")
        print("Resumen del escaneo (global):")
        print(f"Archivos procesados: {processed} (omitidos: {skipped}, errores: {errors_count})")
        print(f"PDFs solo imagen (1): {pdf_1}, con texto (0): {pdf_0}, indeterminado: {pdf_x}")
        print(f"Duración: {dur} | Tasa promedio: {rate:.1f} archivos/segundo")


if __name__ == "__main__":
    main()