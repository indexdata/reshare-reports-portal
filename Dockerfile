FROM python:3.14
ADD . /reports
WORKDIR /reports
RUN pip install -r requirements.txt
EXPOSE 8000
ENTRYPOINT ["./gunicorn.sh"]
