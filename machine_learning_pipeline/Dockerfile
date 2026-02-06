# Build stage
FROM python:3.9-slim as builder

WORKDIR /app

COPY pyproject.toml .
# Install build tools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel build

# Create wheel
COPY src/ src/
RUN python -m build --wheel

# Runtime stage
FROM python:3.9-slim

WORKDIR /app

COPY --from=builder /app/dist/*.whl .
COPY requirements.txt . 
# If requirements.txt exists use it, otherwise depend on the wheel, but here we copied it just in case.
# Actually let's just install the wheel which contains dependencies if properly configured.
# But for simplicity in this hybrid setup:

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY . .

CMD ["python", "main.py"]
