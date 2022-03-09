FROM python:3.6.9
ADD . /reports
WORKDIR /reports
RUN pip install -r requirements.txt
EXPOSE 8000
ENTRYPOINT ["./gunicorn.sh"]
