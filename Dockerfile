FROM python:3

ADD . /

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "data-stream.py"]
