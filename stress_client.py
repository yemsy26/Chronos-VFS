import asyncio
import aiohttp
import time
import struct
import pandas as pd

# Parámetros HFT
URL_BASE = "http://localhost:8081/virtual_file"
TICK_SIZE = 24 # sizeof(MarketTick): u64 (8) + f64 (8) + f64 (8) = 24 bytes
TICKS_PER_REQUEST = 2000 # ~48KB por petición
BYTES_PER_REQUEST = TICKS_PER_REQUEST * TICK_SIZE
TOTAL_REQUESTS = 1000 # Total 2,000,000 de Ticks
MAX_CONCURRENT = 100

async def fetch_sequential(session, semaphore, req_index):
    # En esta prueba secuencial, pedimos los datos en orden estricto
    # simulando un cliente que reproduce un backtest o un feed histórico a velocidad extrema.
    offset = req_index * BYTES_PER_REQUEST
    url = f"{URL_BASE}?offset={offset}&len={BYTES_PER_REQUEST}"
    
    async with semaphore:
        try:
            async with session.get(url) as response:
                data = await response.read()
                return (req_index, data, response.status == 200)
        except Exception as e:
            return (req_index, b"", False)

async def main():
    print("===================================================================")
    print("   NVM Prototype - Ingestión Estructurada Binaria (HFT Ticks)      ")
    print("===================================================================")
    print(f"Descargando {TOTAL_REQUESTS * TICKS_PER_REQUEST:,} Ticks en bloques secuenciales...")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, enable_cleanup_closed=True)
    
    all_ticks = []
    
    start_global = time.perf_counter()
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(fetch_sequential(session, semaphore, i)) for i in range(TOTAL_REQUESTS)]
        
        print("[-] Disparando Peticiones Concurrentes...")
        results = await asyncio.gather(*tasks)
    
    elapsed_net = time.perf_counter() - start_global
    
    # ---------------------------------------------------------
    # FASE DE DESEMPAQUETADO BINARIO ZERO-COPY (Python)
    # ---------------------------------------------------------
    print("[-] Desempaquetando structs C (Zero-Copy JSON Bypass)...")
    
    # Ordenar por req_index para garantizar que ensamblamos el archivo lógico secuencialmente
    results.sort(key=lambda x: x[0])
    
    start_parse = time.perf_counter()
    
    # Formato struct C: '<Qdd' significa:
    # '<' = Little Endian (arquitectura estándar x86_64)
    # 'Q' = unsigned long long (u64, 8 bytes)
    # 'd' = double (f64, 8 bytes)
    # 'd' = double (f64, 8 bytes)
    unpack_format = '<Qdd'
    
    for req_index, data, success in results:
        if success and len(data) > 0:
            # struct.iter_unpack está altamente optimizado en C para decodificar
            # buffers binarios brutos directamente sin pasar por strings UTF-8.
            unpacked = list(struct.iter_unpack(unpack_format, data))
            all_ticks.extend(unpacked)
            
    elapsed_parse = time.perf_counter() - start_parse
    
    # ---------------------------------------------------------
    # VALIDACIÓN MATEMÁTICA Y CARGA A PANDAS
    # ---------------------------------------------------------
    print("[-] Cargando en Pandas DataFrame...")
    df = pd.DataFrame(all_ticks, columns=['timestamp', 'price', 'volume'])
    
    print("\n=== VALIDACIÓN DE INTEGRIDAD ESTRUCTURADA ===")
    print(f"Total Ticks Procesados: {len(df):,}")
    
    if len(df) > 0:
        # Validación de Secuencialidad (Zero-Data-Loss)
        # El StateEngine de Rust configuró el timestamp para que salte exactamente en 100ms.
        diffs = df['timestamp'].diff().dropna()
        is_sequential = (diffs == 100).all()
        
        print(f"Secuencialidad Perfecta (Zero-Data-Loss): {'[SÍ]' if is_sequential else '[NO] Corrupción detectada'}")
        
        print("\n=== RENDIMIENTO DE PIPELINE ===")
        total_mb = (len(df) * TICK_SIZE) / (1024 * 1024)
        print(f"Tiempo Transmisión Red: {elapsed_net:.3f} s  ({total_mb/elapsed_net:.2f} MB/s reales)")
        print(f"Tiempo Decodificación:  {elapsed_parse:.3f} s  ({(len(df)/elapsed_parse):,.0f} Ticks/s decodificados)")
        
        print("\n=== MUESTRA DEL DATAFRAME (Primeros y Últimos Ticks) ===")
        print(df.head(3))
        print("...")
        print(df.tail(3))
    else:
        print("[X] Fallo: El servidor no devolvió datos válidos.")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
