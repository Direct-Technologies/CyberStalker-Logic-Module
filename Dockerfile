FROM python:3.9-slim-buster

COPY --from=pixelcore.azurecr.io/pixctl:latest ./pixctl .

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY start.sh .
COPY dispatcher dispatcher
COPY docs docs

RUN chmod +x start.sh

CMD sleep 1; /start.sh
