import pandas as pd
import os, sys

st_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, st_dir)

from storage.storage import get_df, println, set_path

def rep31(ruta):
    set_path(ruta)
    print("")
    println("__REPORTE(1)__")
    
    print("Obteniendo datos...")
    
    hosts_df = get_df('host')
    logs_df = get_df('log')
    maintenance_df = get_df('maintenance')
    
    println("Datos preparados.")
    
    println("1.1: Cantidad de servidores por país y entorno.")
    
    cant_pais_entorno = (
        hosts_df
        .groupby(['country', 'environment'])
        .size()
        .reset_index(name='count')
        .pivot(index='country', columns='environment', values='count')
    )
    
    println(cant_pais_entorno)
    
    print("1.2: Porcentaje de servidores que usa Linux.")
    
    hosts_linux = hosts_df[hosts_df['os'] == 'Linux'].reset_index()
    
    porcentaje = 0
    
    if len(hosts_df) != 0:
        porcentaje = len(hosts_linux) / len(hosts_df)
    
    println(str(round(porcentaje, 2) * 100) + "%")
    
    #.unstack() transoforma serie a tabla
    println("1.3: Servidores con mayor tiempo promedio de respuesta.")
    print(logs_df.groupby('id_server')['response_time_ms'].mean().sort_values(ascending=False))
    print("")
    
    println("1.4: Tipo de request mas lento.")
    print(logs_df.groupby('request_type')['response_time_ms'].mean().sort_values(ascending=False))
    print("")
    
    println("1.5: Porcentaje de solicitudes que fallan por servidor y país.")
    logs_fallas = logs_df.assign(failed=logs_df['status_code'].astype(int) >= 400)
    
    merge = pd.merge(hosts_df, logs_fallas, how='inner', right_on='id_server', left_index=True)
    
    prom_fallas_servidor_pais = (
        merge
        .groupby(['id_server', 'country'])['failed']
        .mean().sort_values(ascending=False)
        .apply(lambda x: 100*(1-x) if x != None else 0)
        .reset_index(name='mean Failure')
        .pivot(index='id_server', columns='country', values='mean Failure')
    )
    
    println(prom_fallas_servidor_pais)
    println("(Si la celda es NaN, significa que no hay datos para ese servidor en el país)")
    
    println("1.6: Tipo de mantenimiento más largo.")
    print(maintenance_df.groupby('type')['duration_min'].mean().sort_values(ascending=False))
    print("")
    
    println("1.7: Técnico con más intervenciones.")
    print(maintenance_df['technician'].value_counts().head())
    print("")
    
    println("1.8: Horas de mantenimiento invertidas por servidor.")
    horas_m = maintenance_df.groupby('id_server').agg({'duration_min': 'sum'}).apply(lambda x: round(x / 60, 3))
    horas_m = horas_m.rename(columns={'duration_min': 'duration_hour'})
    println(horas_m)
    print("_end")