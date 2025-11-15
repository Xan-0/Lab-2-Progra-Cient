import pandas as pd
import numpy as np
import ollama

np.random.seed(42)

def leer_hosts(path):
    try:
        df = pd.read_csv(path)
        
    except FileNotFoundError:
        print(f"File Not Found at path: {path}")
        return pd.DataFrame()
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()
    
    df['id'] = df.index
    
    return df

def generar_logs(df, n):
    
    id_log = np.arange(n)
    id_server = np.random.choice(df['id'], n)
    
    start = pd.Timestamp.now()
    end = start + pd.Timedelta(days=30)
    
    start_s = start.timestamp()
    end_s = end.timestamp()

    timestamp = pd.to_datetime(np.random.randint(start_s, end_s, size=n), unit='s')
    request_type = np.random.choice(['GET', 'POST', 'PUT', 'DELETE'], n)
    response_time_ms = np.random.randint(10, 10000, size=n)
    status_code = np.random.choice([200, 201, 400, 401, 403, 404, 500], n)
    user = [f'user{num:03d}' for num in np.random.randint(1, 1000, size=n)]
    
    logs = pd.DataFrame({
        'id_log': id_log,
        'id_server': id_server,
        'timestamp': timestamp,
        'request_type': request_type,
        'response_time_ms': response_time_ms,
        'status_code': status_code,
        'user': user
    })
    
    return logs

def generar_maintenance(df, n, tokens):
    
    id_maintenance = np.arange(n)
    id_server = np.random.choice(df['id'], n)
    
    start = pd.Timestamp.now()
    end = start + pd.Timedelta(days=30)
    
    start_s = start.timestamp()
    end_s = end.timestamp()
    
    date = pd.to_datetime(np.random.randint(start_s, end_s, size=n), unit='s')
    _type = np.random.choice(['Patch', 'Incident', 'Upgrade'], n)
    duration_min = np.random.randint(5, 421, size=n)
    technician = [f'tech{num:03d}' for num in np.random.randint(1, 1000, size=n)]
    
    local_prompt = "Generate a concise, generic maintenance note (only 12 words) shortly describing any routine system task—generic."
    notes = []
    
    for i in range(1, n+1):
        if i % 10 == 0:
            print(f"generation:notes ({i}/{n})")
        
        response = ollama.chat(
            model='phi:latest',
            messages = [{'role': 'user', 'content': local_prompt}],
            #cantidad de tokens
            options = {"num_predict": tokens}
        )
        if(len(response['message']['content']) > 2):
            notes.append(response['message']['content'].strip())
            continue
        #Si no genera bien el tamaño, default
        notes.append('(default) Server rebooted successfully.')
    
    maintenance = pd.DataFrame({
        'id_maintenance': id_maintenance,
        'id_server': id_server,
        'date': date,
        'type': _type,
        'duration_min': duration_min,
        'technician': technician,
        'notes': notes
    })
    
    return maintenance