FROM python:3.12.3


COPY . /

WORKDIR /app

# Copia los archivos de requisitos
COPY requirements.txt /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "src/main.py"]