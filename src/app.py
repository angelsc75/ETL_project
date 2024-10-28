import streamlit as st
import requests
import pandas as pd
from PIL import Image
from datetime import datetime
import time

# Configuración de la página
st.set_page_config(layout="wide")

# URL del backend FastAPI
BASE_URL = "http://localhost:8000"

# Inicializar el estado de la sesión para el input de búsqueda
if 'search_input' not in st.session_state:
    st.session_state.search_input = ""
# Función para manejar la búsqueda y limpiar el campo
def handle_submit():
    # Guardar el término de búsqueda actual
    st.session_state.last_search = st.session_state.search_input
    # Limpiar el campo de búsqueda
    st.session_state.search_input = ""

# Función callback para limpiar el campo de búsqueda
def clear_text():
    st.session_state.search_input = ""

# Función para obtener el conteo de registros
@st.cache_data(ttl=60)  # Cache por 60 segundos
def get_row_count():
    try:
        response = requests.get(f"{BASE_URL}/count")
        if response.status_code == 200:
            return response.json().get('total_rows', 0)
        return 0
    except (requests.RequestException, ValueError) as e:
        # Logear el error si tienes configurado logging
        print(f"Error getting row count: {str(e)}")
        return 0

# Función para formatear el DataFrame
def format_dataframe(data):
    df = pd.DataFrame(data)
    column_order = [
        'personal_id', 'fullname', 'email', 'passport', 'sex', 
        'telfnumber', 'location_city', 'location_address', 
        'company_name', 'company_address', 'company_telfnumber', 
        'company_email', 'job_title', 'bank_iban', 'salary_amount', 
        'network_ipv4'
    ]
    df = df[column_order]
    df['salary_amount'] = df['salary_amount'].apply(lambda x: f"${x:,.2f}")
    return df

# Cargar y mostrar la imagen del encabezado
try:
    image = Image.open("header_image.png")
    st.image(image, use_column_width=True)
except FileNotFoundError:
    st.warning("Header image not found. Please add 'header_image.png' to the project directory.")

st.title("HR PRO")

# Mostrar el contador con actualización automática
count = get_row_count()
current_time = datetime.now().strftime("%H:%M:%S")

st.markdown(
    f"""
    <div style='padding: 10px; background-color: #f0f2f6; border-radius: 5px; text-align: center;'>
        <h4>Total de registros en la base de datos: {count:,}</h4>
        <p style='font-size: 0.8em; color: gray;'>Última actualización: {current_time}</p>
    </div>
    """,
    unsafe_allow_html=True
)

# Configurar rerun automático cada 60 segundos
if 'last_run' not in st.session_state:
    st.session_state.last_run = time.time()

current_time = time.time()
if current_time - st.session_state.last_run > 60:
    st.session_state.last_run = current_time
    st.rerun()

# Crear pestañas para las diferentes funcionalidades
tab1, tab2 = st.tabs(["Ver Todos los Datos", "Búsqueda"])

with tab1:
    if st.button("Cargar Todos los Datos"):
        with st.spinner("Cargando datos..."):
            response = requests.get(f"{BASE_URL}/vista")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data:
                        df = format_dataframe(data)
                        st.dataframe(df, width=1000, height=600)
                    else:
                        st.info("No hay datos disponibles.")
                except ValueError as e:
                    st.error(f"Error al procesar los datos: {str(e)}")
            else:
                st.error(f"Error al obtener datos: {response.status_code}")

with tab2:
    # Crear un formulario para la búsqueda
    with st.form(key='search_form'):
        search_term = st.text_input(
            "Buscar por nombre, apellido, nombre completo, teléfono o pasaporte:",
            key='search_input'
        )
        submit_button = st.form_submit_button("Buscar", on_click=handle_submit)
        
        if submit_button:
            if st.session_state.last_search:  # Usar el término guardado
                with st.spinner("Buscando..."):
                    response = requests.get(
                        f"{BASE_URL}/search",
                        params={"search_term": st.session_state.last_search}
                    )
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if not data:
                                st.info("No se encontraron resultados.")
                            else:
                                df = format_dataframe(data)
                                st.dataframe(df, width=1000, height=600)
                        except ValueError as e:
                            st.error(f"Error al procesar los resultados: {str(e)}")
                    else:
                        st.error(f"Error en la búsqueda: {response.status_code}")
            else:
                st.warning("Por favor ingrese un término de búsqueda.")