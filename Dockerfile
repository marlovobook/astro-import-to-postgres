FROM quay.io/astronomer/astro-runtime:10.4.0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt