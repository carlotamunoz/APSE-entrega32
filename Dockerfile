FROM python:3.11-slim-bullseye
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install flask
COPY . .
WORKDIR /service
EXPOSE 7777
CMD [ "python3", "prediction_service.py", "--host=0.0.0.0"]
