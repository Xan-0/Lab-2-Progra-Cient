from storage.storage import insert_df, println, set_path, create_base_tables
from generation.generation import leer_hosts, generar_logs, generar_maintenance
import config.config as configuracion
from reports.report_r31 import rep31
from reports.report_r32 import rep32_ALL
from reports.report_r33 import rep33
import sys

print("")
println("__MAIN__")

#cantidad de filas a generar
n = configuracion.n
tokens = configuracion.tokens

df = leer_hosts(configuracion.ruta)

if(df.empty):
    print("ERROR (main), dataframe vacio")
    #termina tempranamente el codigo
    sys.exit()

print("HOSTS_DF:")
println(df)

logs = generar_logs(df, n)

print("LOGS_DF:")
println(logs)

maintenance = generar_maintenance(df, n, tokens)

print("")
print("MAINTENANCE_DF:")
println(maintenance)

#Guarda dataframes generados en servidor sqlite3
ruta = 'storage/infrastructure.db'
create_base_tables()
set_path(ruta)
insert_df('host', df)
insert_df('log', logs)
insert_df('maintenance', maintenance)

#REPORTES
rep31(ruta)
rep32_ALL(ruta)
rep33(ruta)