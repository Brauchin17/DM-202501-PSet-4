
FROM python:3.10

WORKDIR /app

# Copiamos el script y el requirements
COPY notebooks/ /app/notebooks/
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

ENTRYPOINT ["python", "/app/notebooks/build_obt.py"]