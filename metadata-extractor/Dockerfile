FROM python:3.6

# Install renku-python
RUN pip install -e git+https://github.com/SwissDataScienceCenter/renku-python.git#egg=renku

# Add script
ADD extract_metadata.py /work/extract_metadata.py

CMD ["python", "/work/extract_metadata.py"]
