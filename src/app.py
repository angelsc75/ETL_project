import streamlit as st
import requests
import pandas as pd

st.title("Streamlit Frontend")

# URL del backend FastAPI
backend_url = "http://localhost:8000/vista"  # Ruta correcta de FastAPI

st.write("Fetching data from FastAPI...")

# Fetch data from FastAPI
response = requests.get(backend_url)
if response.status_code == 200:
    data = response.json()
    
    # Convertir los datos a un DataFrame de pandas
    df = pd.DataFrame(data)
    
    # Mostrar los datos como una tabla interactiva en Streamlit
    st.dataframe(df)  # También puedes usar st.table(df) si prefieres una tabla estática
else:
    st.write("Failed to fetch data from API")
