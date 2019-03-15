FROM python:3.6-slim

WORKDIR /app

COPY . /app
RUN pip install -r requirements.txt

EXPOSE 333

CMD [ "python", "./get_ba2_data.py" ]

