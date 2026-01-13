# Usa un'immagine Python leggera
FROM python:3.11-slim

# Imposta la cartella di lavoro nel container
WORKDIR /app

# Copia i file necessari
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tutto il resto del progetto
COPY . .

# Espone la porta usata da Streamlit
EXPOSE 8501

# Comando per avviare la dashboard
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]