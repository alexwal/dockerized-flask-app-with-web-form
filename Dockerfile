FROM tiangolo/uwsgi-nginx-flask:python3.6-alpine3.7
RUN pip install wtforms
ENV STATIC_INDEX 0
COPY ./app /app

