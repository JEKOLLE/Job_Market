FROM python:3.9

COPY requirements.txt requirements.txt

RUN pip install --no-cache -r requirements.txt

EXPOSE 8000

COPY api.py api.py

CMD ["uvicorn", "api:app", "--host", "0.0.0.0"]