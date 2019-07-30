FROM python:3.7
MAINTAINER Sergey Bogoslovskiy <bogoslovskiysergeyw@gmail.com>

# install system dependencies(jessie)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libatlas-base-dev \
    gfortran \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

ADD requirements.txt /requirements.txt

# install python dependencies
RUN pip install -r /requirements.txt

RUN pip install git+https://github.com/bogoslovskiysergey/util

RUN mkdir /logging

ADD main.py /cmd/main.py

# kostyl for local testing on MacBook. Sorry!
ENV GOOGLE_APPLICATION_CREDENTIALS=/configs/prod_bigquery.json

CMD ["python", "/cmd/main.py"]