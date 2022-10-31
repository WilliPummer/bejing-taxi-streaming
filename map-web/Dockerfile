FROM python:3.10.7-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD [ "python3","-u", "webapp.py"]

CMD ["gunicorn", "--workers" ,"4","--timeout","3600", "--bind", "0.0.0.0:5000", "wsgi:app"]