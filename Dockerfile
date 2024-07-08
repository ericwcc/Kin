FROM        python:3.8.19-slim
WORKDIR     /opt/kin
COPY        requirements.txt requirements.txt
RUN         pip install -r requirements.txt
COPY        src .
ENTRYPOINT  ["python", "kin.py"]