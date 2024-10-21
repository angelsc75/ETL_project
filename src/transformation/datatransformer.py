import json
import re
import phonenumbers

def standardize_phone(phone_number):
    """ Valida y formatea los números de teléfono usando la librería phonenumbers """
    if phone_number:
        try:
            # Parsear el número con phonenumbers
            try:
                phone_obj = phonenumbers.parse(phone_number)
                
                # Validar si el número de teléfono es válido
                is_valid = phonenumbers.is_valid_number(phone_obj)
                
                # Formatear el número en formato internacional si es válido
                formatted_number = phonenumbers.format_number(phone_obj, phonenumbers.PhoneNumberFormat.INTERNATIONAL) if is_valid else None
                return formatted_number
            
            except phonenumbers.NumberParseException:
                is_valid = False
                formatted_number = None

        except phonenumbers.NumberParseException:
            return None  # Si el número no se puede parsear, devolver None
    return None

def separate_salary_currency(salary):
    """ Separa el salario y la moneda """
    if salary:
        match = re.match(r'([\d,\.]+)([^\d,]+)', salary)
        if match:
            amount = match.group(1).replace(',', '')  # Remover comas de miles
            currency = match.group(2).strip()
            return float(amount), currency
        else:
            return float(salary.replace(',', '')), None  # Asumir sin moneda si no se encuentra
    return None, None

def validate_email(email):
    """ Valida el formato del correo electrónico """
    if email and re.match(r'[^@]+@[^@]+\.[^@]+', email):
        return email
    return None

def normalize_gender(sex):
    """ 
    Normaliza el género a 'M', 'F' o 'ND' (no definido).
    Maneja casos donde sex puede ser una lista o un string.
    """
    if sex:
        # Si 'sex' es una lista, tomar el primer elemento
        if isinstance(sex, list) and len(sex) > 0:
            sex = sex[0]
        
        # Verificar si el valor es un string antes de aplicar upper()
        if isinstance(sex, str):
            if sex.upper() in ['M', 'F']:
                return sex.upper()
            else:
                return 'ND'  # No definido si no es 'M' o 'F'
    
    return 'ND'  # No definido si sex es None o no válido

def is_valid_passport(passport_number):
    # Una expresión regular genérica para pasaportes
    pattern = r'^[A-Z0-9]{6,9}$'  # Generalmente entre 6 y 9 caracteres alfanuméricos
    return bool(re.match(pattern, passport_number))
    
def validate_generic_passport_format(passport_number):
    is_valid = is_valid_passport(passport_number)  
    if is_valid:
        try:    
            return passport_number
        except re.error:
             return None  # Si el pasaporte no es válido, devolver None
    else:
        return None



def process_and_group_data(raw_message):
    """
    Transforma y agrupa los datos del mensaje crudo en las categorías definidas.
    """
    try:
        # Supongamos que el mensaje ya viene en formato JSON
        data = json.loads(raw_message)
    except json.JSONDecodeError as e:
        # Manejar el error si el JSON no es válido
        print(f"Error al decodificar JSON: {e}")
        return {"error": "Invalid JSON format"}

    # Extraer diferentes categorías de datos
    if "city" in data or "country" in data:
        location_data = {
            "fullname": data.get("fullname"),
            "address": data.get("address"),
            "city": data.get("city"),
            "country": data.get("country")
        }
        return location_data
    
    elif "name" in data or "last_name" in data:
        personal_data = {
            "name": data.get("name"),
            "last_name": data.get("last_name"),
            "fullname": f"{data.get('name', '')} {data.get('last_name', '')}".strip(),
            "telfnumber": standardize_phone(data.get("telfnumber")),
            "passport": validate_generic_passport_format(data.get("passport")),
            "email": validate_email(data.get("email")),
            "sex": normalize_gender(data.get("sex"))  # Corregido para usar normalize_gender
        }
        return personal_data

    elif "job" in data or "company" in data:
        professional_data = {
            "fullname": data.get("fullname"),
            "company": data.get("company"),
            "job": data.get("job"),
            "company_telfnumber": standardize_phone(data.get("company_telfnumber")),
            "company_address": data.get("company_address"),
            "company_email": validate_email(data.get("company_email"))
        }
        return professional_data

    elif "IBAN" in data or "salary" in data:
        salary, currency = separate_salary_currency(data.get("salary"))
        bank_data = {
            "passport": data.get("passport"),
            "IBAN": data.get("IBAN"),
            "salary": salary,
            "currency": currency
        }
        return bank_data

    elif "IPv4" in data:
        net_data = {
            "address": data.get("address"),
            "IPv4": data.get("IPv4")
        }
        return net_data

    # Si no hay ningún campo esperado en el mensaje, devolver un error manejable
    return {"error": "No valid data found"}
