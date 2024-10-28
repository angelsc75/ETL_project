import streamlit as st
import requests
import pandas as pd

st.title("HR PRO")

# URL del backend FastAPI
backend_url = "http://localhost:8000/vista"  # Ruta correcta de FastAPI

# Mostrar un spinner mientras se cargan los datos
with st.spinner("Fetching data from FastAPI..."):
    response = requests.get(backend_url)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    try:
        data = response.json()
        
        # Convertir los datos a un DataFrame de pandas
        df = pd.DataFrame(data)

        # Reorganizar el orden de las columnas según tu preferencia
        column_order = ['personal_id', 'fullname', 'email', 'passport', 'sex', 'telfnumber',
                        'location_city', 'location_address', 'company_name', 'company_address',
                        'company_telfnumber', 'company_email', 'job_title', 'bank_iban', 'salary_amount', 
                        'network_ipv4']
        df = df[column_order]
        
        # Dar formato a la columna de salario (si es un número)
        df['salary_amount'] = df['salary_amount'].apply(lambda x: f"${x:,.2f}")      

        
        # Mostrar los datos como una tabla interactiva en Streamlit
        st.dataframe(df, width=1000, height=600)  # Puedes ajustar las dimensiones según sea necesario

    except ValueError:
        st.error("Error al procesar los datos del API. Verifique el formato de los datos.")
else:
    st.error(f"Failed to fetch data from API. Status code: {response.status_code}")
