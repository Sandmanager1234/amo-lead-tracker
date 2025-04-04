FROM python:3.11-alpine

WORKDIR /app

COPY . /app

RUN pip install -r req.txt

CMD [ "python", "main.py" ]


