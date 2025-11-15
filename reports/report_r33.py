import os, sys

st_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, st_dir)

from storage.storage import println, read_query, set_path

def rep33(ruta):
    set_path(ruta)
    print("")
    println("__REPORTE(3)__")
    
    print("3.1: Cantidad de servidores por país y entorno.")
    servs_pais_entorno = read_query(
        """
        SELECT country, environment, COUNT(*) as count
        FROM host
        GROUP BY country, environment
        """
    )
    
    println(servs_pais_entorno)
    
    print("3.2: Sistemas operativos que dominan en producción.")
    os_predominantes = read_query(
        """
        SELECT os, COUNT(*) as count
        FROM host
        GROUP BY os
        ORDER BY count DESC
        """
    )
    println(os_predominantes)
    
    print("3.3: Servidor con más mantenimientos registrados (No muestra empates).")
    server_mayor = read_query(
        """
        SELECT id_server, COUNT(*) as count
        FROM maintenance
        GROUP BY id_server
        ORDER BY count DESC
        """
    )
    
    println(server_mayor.iloc[0])
    
    print("3.4: Entornos que presentan más errores HTTP")
    entornos_mayor_error = read_query(
        """
        SELECT environment, COUNT(status_code) as count
        FROM host
        INNER JOIN log ON host.id = log.id_server
        WHERE status_code>399
        GROUP BY environment
        ORDER BY count DESC
        """
    )
    
    println(entornos_mayor_error)
    
    print("3.5: Técnico que ha trabajado en más servidores de producción (No muestra empates)")
    tecnico_mayor = read_query(
        """
        SELECT technician, COUNT(id_server) as count
        FROM maintenance
        GROUP BY technician
        ORDER BY count DESC
        """
    )
    
    println(tecnico_mayor.iloc[0])
    
    print("3.6: Solicitudes totales que maneja cada país por mes.")
    solicitudes_pais_mes = read_query(
        """
        SELECT country, strftime('%m', timestamp) as month, COUNT(*) as count
        FROM host
        INNER JOIN log ON host.id = log.id_server
        GROUP BY country, month
        """
    )
    
    println(solicitudes_pais_mes)
    print("_end")