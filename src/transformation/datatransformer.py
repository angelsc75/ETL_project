import json
import re
import phonenumbers

def standardize_phone(telfnumber):
    """ Valida y formatea los números de teléfono usando la librería phonenumbers """
    if telfnumber:
        try:
            # Parsear el número sin una región predeterminada
            # phone_obj = phonenumbers.parse(telfnumber)

            # # Validar si el número de teléfono es válido
            # if phonenumbers.is_valid_number(phone_obj):
            #     # Obtener la región del número
            #     region = phonenumbers.region_code_for_number(phone_obj)
                
            #     # Formatear el número en formato internacional
            #     return phonenumbers.format_number(phone_obj, phonenumbers.PhoneNumberFormat.INTERNATIONAL), region
            # else:
            #     return None, None  # Si el número no es válido, devolver None

            # Número de teléfono a verificar
            phone_number = telfnumber

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
    """ Normaliza el género a 'M', 'F' o 'ND' (no definido) """
    if sex:
        if sex.upper() in ['M', 'F']:
            return sex.upper()
        else:
            return 'ND'  # No definido
    return 'ND'


def process_and_group_data(raw_message):
    """
    Transforma y agrupa los datos del mensaje crudo en las categorías definidas.
    """
    # Supongamos que el mensaje ya viene en formato JSON
    data = json.loads(raw_message)

    # Extraer diferentes categorías de datos
    message = data

    # Manejar location_data solo si existe "city" o algún campo relacionado
    # location_data = {}
    # personal_data = {}
    # professional_data = {}
    # bank_data = {}
    # net_data = {}

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
            "telfnumber": standardize_phone(data.get("telfnumber")),
            "telfnumber_raw": data.get("telfnumber"),
            "passport": data.get("passport"),
            "email": validate_email(data.get("email")),
            "sex": data.get("sex")[0] if data.get("sex") else None 
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

    # # Agrupar todos los datos en un solo registro
    # grouped_data = {
    #     "message": message,
    #     "personal_data": personal_data,
    #     "location_data": location_data,
    #     "professional_data": professional_data,
    #     "bank_data": bank_data,
    #     "net_data": net_data
    # }

    # return grouped_data
