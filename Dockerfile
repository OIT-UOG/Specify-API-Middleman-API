FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7
COPY REQUIREMENTS.txt /
RUN pip3 install -r /REQUIREMENTS.txt
COPY ./app /app/app
ENV PORT=8000