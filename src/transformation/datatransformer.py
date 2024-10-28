"""
Este módulo implementa funciones especializadas para la validación, transformación y normalización
de diferentes tipos de datos comúnmente encontrados en sistemas de procesamiento de información.

El módulo está diseñado para ser utilizado como parte de un pipeline de procesamiento de datos
más grande, trabajando en conjunto con componentes como:
- KafkaConsumer: Para la ingesta de datos
- RedisLoader: Para caché y almacenamiento temporal
- MongoDBLoader: Para almacenamiento de datos no estructurados
- SQLDBLoader: Para almacenamiento de datos estructurados

Características principales:
- Validación y formateo de números telefónicos
- Procesamiento de información salarial y conversión de monedas
- Validación de correos electrónicos
- Normalización de datos de género
"""

# Importación de bibliotecas necesarias
import json          # Para manejo de datos en formato JSON
import re            # Para procesamiento de expresiones regulares
import phonenumbers  # Biblioteca especializada para manejo de números telefónicos

def standardize_phone(phone_number):
    """ 
    Normaliza y valida números telefónicos convirtiéndolos al formato internacional estándar.
    
    Proceso de la función:
    1. Recibe un número telefónico en cualquier formato
    2. Intenta parsearlo usando la biblioteca phonenumbers
    3. Valida el número según estándares internacionales
    4. Si es válido, lo convierte al formato internacional
    
    Args:
        phone_number (str): Número telefónico que puede estar en cualquier formato
                           Ejemplos: "+1234567890", "123-456-7890", "(123) 456-7890"
    
    Returns:
        str or None: Retorna el número formateado si es válido (ej: "+1 234-567-890")
                    None si el número no es válido o no se puede procesar
    
    Ejemplos de uso:
        >>> standardize_phone("+1234567890")
        "+1 234-567-890"
        >>> standardize_phone("número inválido")
        None
    """
    # Verificar que se recibió un número (no None o vacío)
    if phone_number:
        try:
            # Bloque principal de procesamiento del número
            try:
                # Convertir el string a un objeto PhoneNumber para su manipulación
                # phonenumbers.parse() analiza el string y crea un objeto estructurado
                phone_obj = phonenumbers.parse(phone_number)
                
                # Verificar si el número cumple con las reglas de validación internacional
                # Esto incluye verificar longitud correcta, código de país válido, etc.
                is_valid = phonenumbers.is_valid_number(phone_obj)
                
                # Si el número es válido, convertirlo al formato internacional estándar
                # Si no es válido, asignar None
                formatted_number = phonenumbers.format_number(
                    phone_obj, 
                    phonenumbers.PhoneNumberFormat.INTERNATIONAL
                ) if is_valid else None
                
                # Retornar el número formateado (o None si no era válido)
                return formatted_number
            
            except phonenumbers.NumberParseException:
                # Si el número no se puede parsear (formato irreconocible)
                # retornar None indicando que no es un número válido
                return None

        except phonenumbers.NumberParseException:
            # Captura adicional de errores de parsing para mayor robustez
            # Este catch extra asegura que cualquier error de parsing sea manejado graciosamente
            return None
    # Si el input era None o vacío, retornar None
    return None

def separate_salary_currency(salary):
    """ 
    Procesa un string que contiene información salarial y separa el monto numérico
    de su código de moneda correspondiente.
    
    Proceso de la función:
    1. Recibe un string con formato "MONTO[MONEDA]"
    2. Separa el monto numérico de la moneda usando regex
    3. Limpia el monto (elimina separadores de miles)
    4. Convierte el monto a float
    
    Args:
        salary (str): String conteniendo el salario
                     Formatos aceptados: "1,000.00USD", "1000EUR", "1,000"
    
    Returns:
        tuple: (float, str) conteniendo:
               - float: monto numérico del salario
               - str: código de moneda (o None si no se especificó moneda)
               
    Ejemplos de uso:
        >>> separate_salary_currency("1,000.00USD")
        (1000.00, "USD")
        >>> separate_salary_currency("1000")
        (1000.00, None)
    """
    # Verificar que se recibió un valor de salario
    if salary:
        # Usar regex para buscar un patrón de números seguido opcionalmente por texto
        # ([\d,\.]+) captura uno o más dígitos, comas o puntos
        # ([^\d,]+) captura uno o más caracteres que no sean dígitos ni comas (la moneda)
        match = re.match(r'([\d,\.]+)([^\d,]+)', salary)
        
        if match:
            # Si se encontró el patrón esperado:
            # Extraer y limpiar el monto numérico (grupo 1 del regex)
            # replace(',', '') elimina las comas de los separadores de miles
            amount = match.group(1).replace(',', '')
            
            # Extraer y limpiar el código de moneda (grupo 2 del regex)
            # strip() elimina espacios en blanco al inicio y final
            currency = match.group(2).strip()
            
            # Retornar la tupla con el monto convertido a float y el código de moneda
            return float(amount), currency
        else:
            # Si no se encontró moneda, asumir que todo el string es el monto
            # Convertir a float después de limpiar separadores de miles
            return float(salary.replace(',', '')), None
            
    # Si el input era None o vacío, retornar (None, None)
    return None, None

def validate_email(email):
    """ 
    Valida que una cadena de texto tenga un formato básico de correo electrónico válido.
    
    Proceso de la función:
    1. Verifica que el email no sea None o vacío
    2. Aplica una expresión regular para validar formato básico
    3. Retorna el email si es válido, None si no lo es
    
    La validación verifica:
    - Presencia de un caracter @ 
    - Texto antes y después del @
    - Al menos un punto después del @
    
    Args:
        email (str): String que supuestamente contiene una dirección de email
                    Ejemplo: "usuario@dominio.com"
    
    Returns:
        str or None: El mismo email si es válido, None si no lo es
        
    Ejemplos de uso:
        >>> validate_email("usuario@dominio.com")
        "usuario@dominio.com"
        >>> validate_email("correo_invalido")
        None
    """
    # Verificar que el email no sea None y que cumpla con el patrón básico:
    # [^@]+ : uno o más caracteres que no sean @
    # @ : el símbolo @
    # [^@]+ : uno o más caracteres que no sean @
    # \. : un punto
    # [^@]+ : uno o más caracteres que no sean @
    if email and re.match(r'[^@]+@[^@]+\.[^@]+', email):
        return email
    return None

def normalize_gender(sex):
    """ 
    Normaliza diferentes formatos de entrada de género a un formato estándar.
    
    Proceso de la función:
    1. Verifica si el input no es None
    2. Maneja casos donde el input es una lista
    3. Normaliza strings a mayúsculas
    4. Valida contra valores permitidos
    
    Args:
        sex (Union[str, list, None]): Valor de género que puede ser:
            - String: "M", "F", "Male", "Female", etc.
            - Lista: ["M"], ["Female"], etc.
            - None: Valor nulo
            
    Returns:
        str: Uno de los siguientes valores:
            - "M": Para género masculino
            - "F": Para género femenino
            - "ND": Para casos no definidos o inválidos
            
    Ejemplos:
        >>> normalize_gender("m")
        "M"
        >>> normalize_gender(["Female"])
        "F"
        >>> normalize_gender("Otro")
        "ND"
    """
    # Primer nivel de validación: verificar que el input no sea None
    if sex:
        # Manejar caso especial: si el input es una lista
        # Esto es útil cuando los datos vienen de fuentes que pueden enviar múltiples valores
        if isinstance(sex, list) and len(sex) > 0:
            # Tomar solo el primer elemento de la lista
            # Ignoramos otros elementos para mantener consistencia
            sex = sex[0]
        
        # Verificar que el valor sea un string antes de procesarlo
        # Esto evita errores al llamar .upper() en tipos no string
        if isinstance(sex, str):
            # Convertir a mayúsculas y verificar si es un valor válido
            # Solo aceptamos 'M' o 'F' como valores válidos
            if sex.upper() in ['M', 'F']:
                return sex.upper()
            else:
                # Cualquier otro string se considera no definido
                return 'ND'  
    
    # Retornar 'ND' para cualquier caso no manejado:
    # - Input es None
    # - Lista vacía
    # - Tipos de datos no soportados
    return 'ND'

def is_valid_passport(passport_number):
    """
    Valida si un número de pasaporte cumple con el formato básico esperado.
    
    Criterios de validación:
    - Longitud entre 6 y 9 caracteres
    - Solo letras mayúsculas y números
    - No espacios ni caracteres especiales
    
    Args:
        passport_number (str): Número de pasaporte a validar
        
    Returns:
        bool: True si el formato es válido, False en caso contrario
        
    Ejemplos:
        >>> is_valid_passport("ABC123")
        True
        >>> is_valid_passport("12345")
        False
    """
    # Definir el patrón de validación usando expresión regular:
    # ^ : inicio del string
    # [A-Z0-9] : solo letras mayúsculas y números
    # {6,9} : longitud entre 6 y 9 caracteres
    # $ : fin del string
    pattern = r'^[A-Z0-9]{6,9}$'
    
    # Retornar True si hay match, False si no
    return bool(re.match(pattern, passport_number))
    
def validate_generic_passport_format(passport_number):
    """
    Valida y retorna un número de pasaporte si cumple con el formato esperado.
    
    Proceso de la función:
    1. Valida el formato usando is_valid_passport
    2. Si es válido, retorna el número original
    3. Si no es válido, retorna None
    
    Args:
        passport_number (str): Número de pasaporte a validar
        
    Returns:
        str or None: Número de pasaporte si es válido, None si no lo es
        
    Ejemplos:
        >>> validate_generic_passport_format("ABC123")
        "ABC123"
        >>> validate_generic_passport_format("12")
        None
    """
    # Validar el formato del pasaporte
    is_valid = is_valid_passport(passport_number)
    
    if is_valid:
        try:
            # Si el formato es válido, retornar el número original
            return passport_number
        except re.error:
            # Capturar cualquier error inesperado en el procesamiento
            # de expresiones regulares
            return None
    else:
        # Si el formato no es válido, retornar None
        return None

def process_and_group_data(raw_message):
    """
    Procesa y agrupa datos crudos en categorías predefinidas.
    
    Este método actúa como un router que:
    1. Parsea el JSON de entrada
    2. Identifica la categoría de los datos
    3. Agrupa y valida los campos correspondientes
    
    Categorías soportadas:
    - Datos de ubicación (location_data)
    - Datos personales (personal_data)
    - Datos profesionales (professional_data)
    - Datos bancarios (bank_data)
    - Datos de red (net_data)
    
    Args:
        raw_message (str): String JSON con los datos a procesar
        
    Returns:
        dict: Diccionario con los datos procesados y agrupados
              o un diccionario de error si algo falla
    
    Ejemplos:
        >>> process_and_group_data('{"name": "John", "email": "john@example.com"}')
        {"name": "John", "email": "john@example.com", ...}
    """
    try:
        # Intentar decodificar el mensaje JSON
        # Si el formato no es válido, se capturará la excepción
        data = json.loads(raw_message)
    except json.JSONDecodeError as e:
        # Logging del error específico para debugging
        print(f"Error al decodificar JSON: {e}")
        # Retornar un error manejable
        return {"error": "Invalid JSON format"}

    # ROUTER DE CATEGORÍAS
    # Cada bloque elif actúa como un router que identifica
    # la categoría de los datos y los procesa acordemente
    
    # 1. Datos de ubicación
    if "city" in data or "country" in data:
        location_data = {
            "fullname": data.get("fullname"),        # Nombre completo
            "address": data.get("address"),          # Dirección
            "city": data.get("city"),               # Ciudad
            "country": data.get("country")          # País
        }
        return location_data
    
    # 2. Datos personales
    elif "name" in data or "last_name" in data:
        personal_data = {
            "name": data.get("name"),
            "last_name": data.get("last_name"),
            # Concatenar nombre y apellido, eliminar espacios extra
            "fullname": f"{data.get('name', '')} {data.get('last_name', '')}".strip(),
            # Validar y formatear número de teléfono
            "telfnumber": standardize_phone(data.get("telfnumber")),
            # Validar formato de pasaporte
            "passport": validate_generic_passport_format(data.get("passport")),
            # Validar formato de email
            "email": validate_email(data.get("email")),
            # Normalizar género
            "sex": normalize_gender(data.get("sex"))
        }
        return personal_data

    # 3. Datos profesionales
    elif "job" in data or "company" in data:
        professional_data = {
            "fullname": data.get("fullname"),
            "company": data.get("company"),
            "job": data.get("job"),
            # Validar teléfono de la compañía
            "company_telfnumber": standardize_phone(data.get("company_telfnumber")),
            "company_address": data.get("company_address"),
            # Validar email de la compañía
            "company_email": validate_email(data.get("company_email"))
        }
        return professional_data

    # 4. Datos bancarios
    elif "IBAN" in data or "salary" in data:
        # Separar el salario en monto y moneda
        salary, currency = separate_salary_currency(data.get("salary"))
        bank_data = {
            "passport": data.get("passport"),
            "IBAN": data.get("IBAN"),
            "salary": salary,
            "currency": currency
        }
        return bank_data

    # 5. Datos de red
    elif "IPv4" in data:
        net_data = {
            "address": data.get("address"),
            "IPv4": data.get("IPv4")
        }
        return net_data

    # Si no se identifica ninguna categoría, retornar error
    return {"error": "No valid data found"}
