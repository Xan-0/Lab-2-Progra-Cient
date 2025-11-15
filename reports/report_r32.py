import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import os, sys

st_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, st_dir)

from storage.storage import get_df, println, set_path

def rep2_1(hosts_df):
    print("Generando Gráficos (2x2)...")
    
    fig, axes = plt.subplots(2, 2, figsize=(20, 14))
    
    cant_pais_so = (
        hosts_df
        .groupby(['country', 'os']) #para agrupacion multiple
        .size()
        .reset_index(name='count')
        .pivot(index='country', columns='os', values='count')
    )
    
    cant_so = hosts_df['os'].value_counts()
    cant_host_pais = hosts_df['country'].value_counts()
    
    cant_entorno_pais = (
        hosts_df
        .groupby(['environment', 'country'])
        .size()
        .reset_index(name='count')
        .pivot(index='environment', columns='country', values='count')
    )
    
    cant_pais_so.plot(kind='barh', ax=axes[0, 0],title='Type of OS grouped by country'
                      ,xlabel='Number of Hosts',ylabel='Country')
    
    cant_so.plot(kind='pie', ax=axes[0, 1], title='Total Operating Systems',color=['green','orange','blue','red']
                 ,autopct=lambda x: round(sum(cant_so)*x/100), pctdistance=1.1, startangle=105
                 ,legend=False, labels=None)
    
    axes[0,1].set_ylabel('') #editar, debiese quitar los textos de categoria del pie chart
    
    vals = [int(val) for val in cant_so.values]
    labls = list(cant_so.index)
    axes[0,1].legend(title='OS', loc='upper right', labels=[f"{label} ({(100*num/sum(cant_so)):.2f}%)" for label, num in zip(labls,vals)])
    
    cols = ['#46327e', '#365c8d', '#277f8e', '#1fa187', '#4ac16d', '#a0da39']
    cant_host_pais.plot(kind='barh', ax=axes[1, 0], color=cols ,title='Total hosts by country'
                        , xlabel='Number of Hosts', ylabel='Country')
    
    cant_entorno_pais.plot(kind='bar', ax=axes[1, 1], xlabel='Environment'
                           , ylabel='Number of Hosts')
    
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1, wspace=0.15, hspace=0.25)
    
    plt.show()
    
    println("Graficos generados.")

def rep2_2(logs_df):
    print("Generando scatter plot (hora petición/tiempo respuesta)...")
    
    plt.figure(figsize=(8, 6))
    
    horas = [ round(int(t[11:].split(":")[0]) + int(t[11:].split(":")[1]) /60
             + int(t[11:].split(":")[2]) /3600, 2) for t in logs_df['timestamp']
    ]
    
    tRespuestas = [ int(ms) for ms in logs_df['response_time_ms']]
    
    
    plt.scatter(horas, tRespuestas, c='blue', marker='o', edgecolors='black', alpha=0.7)
    
    plt.title("Hour of petitions vs Response time")
    plt.xlabel("Time of the day")
    plt.ylabel("Response time ms")
    
    plt.show()
    println("Gráfico generado")

def rep2_3(maintenance_df):
    print("Genrando un WordCloud de las Notas de Mantenimiento...")
    
    notas="" #string acumulado
    
    for nota in maintenance_df['notes']:
        notas+= nota + " "
    
    #Configuracion del WordCloud
    wc = WordCloud(
            width=800,
            height=400,
            background_color="white",
            stopwords=STOPWORDS,
            colormap="vanimo",
            max_words=200
    )
    
    wc.generate(notas) #Genera el WordCloud
    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout()
    plt.show()
    
    println("\nWordCloud Generado.")

def rep2_4(hosts_df, logs_df, maintenance_df):
    print("Generando Gráficos de duración Mantenciones vs tiempo de respuesta por país...")
    merge_logs = pd.merge(hosts_df, logs_df, how='inner', right_on='id_server', left_index=True).reset_index().drop(['timestamp','status_code','request_type','user','id_log','environment','hostname','os','id','node','index_y','index_x'], axis=1)
    merge_maint = pd.merge(hosts_df, maintenance_df, how='inner', right_on='id_server', left_index=True).reset_index().drop(['date','type','technician','notes','id_maintenance','environment','hostname','os','id','node','index_y','index_x'], axis=1)
    merge_logs_maint = pd.merge(merge_logs, merge_maint, how='inner', on=['country', 'id_server']).drop(['index_x','index_y'], axis=1)
    
    if(merge_logs_maint['country'].nunique() == merge_logs_maint['country'].size):
        print("\nAdvertencia, no hay compatibilidad entre los datos generados de logs y mantenimeinto (id_server)\nConsiderar elegir un n más grande en config.py\n")
    
    fig, ax = plt.subplots()
    
    for country, group_df in merge_logs_maint.groupby('country'):
        ax.plot(group_df['response_time_ms'], group_df['duration_min'], label=country)
    
    ax.set_xlabel('Response time (ms)')
    ax.set_ylabel('Maintenance duration (min)')
    ax.set_title('Maintenance duration (min) vs Response time (ms) by Country')
    ax.legend()
    
    plt.show()
    
    println("Gráfico generado")

def rep2_5(logs_df):
    print("Generando gráfico de variación del tiempo de respuesta promedio por mes...")
    
    meses_dict = {1: 'Jan', 2: 'feb', 3: 'Mar', 4: 'Apr', 5: 'May'
                 ,6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct'
                 ,11: 'Nov', 12: 'Dec'}
    
    meses = logs_df['timestamp'].transform(lambda t: int(t.split(' ')[0].split('-')[1]))
    
    mes_tiempo_prom = (
        logs_df.assign(mes=meses)
        .groupby('mes')['response_time_ms'].mean()
        .reset_index()
    )
    
    mes_tiempo_prom['mes'] = mes_tiempo_prom['mes'].transform(lambda meses: pd.Series([meses_dict[m] for m in meses]))
    
    mes_tiempo_prom.plot(x='mes', y='response_time_ms'
                         ,kind='bar',title='Mean response time per month'
                         ,xlabel='Month',ylabel='Mean response time (ms)'
                         ,colormap='viridis')
    
    plt.show()
    
    println("Gráfico generado")

def rep32_ALL(ruta):
    set_path(ruta)
    
    print("")
    println("__REPORTE(2)__")

    print("Obteniendo datos...")

    hosts_df = get_df('host')
    logs_df = get_df('log')
    maintenance_df = get_df('maintenance')

    println("Datos preparados.")
    
    rep2_1(hosts_df)
    rep2_2(logs_df)
    rep2_3(maintenance_df)
    rep2_4(hosts_df, logs_df, maintenance_df)
    rep2_5(logs_df)
    print("_end")
