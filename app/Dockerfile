FROM python:3.5-alpine


ADD . /code
WORKDIR /code


RUN apk update && apk add postgresql-dev gcc musl-dev
RUN pip install -r requirements.txt
RUN pip install --upgrade sqlalchemy==1.2.0b3

# Run app.py when container is launched
CMD ["python", "app.py"]