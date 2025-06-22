FROM python:3.9-slim

# Set the working directory
WORKDIR /sentiment_analysis

# Copy the application files
COPY sentiment_analysis.py .

# Install dependencies
RUN pip install streamlit sqlalchemy textblob psycopg2-binary

# Expose the Streamlit port
EXPOSE 8051

# Command to run the app
CMD ["streamlit", "run", "sentiment_analysis.py", "--server.port=8051", "--server.enableCORS=false"]
