FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /django-app/
COPY ./requirements.txt /django-app/
RUN pip install -r requirements.txt
COPY ./ /django-app/
