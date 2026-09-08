FROM python:3.12-slim

WORKDIR /opt/project
COPY src/generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY src /opt/project/src

CMD ["python", "-m", "src.generator.generate"]
