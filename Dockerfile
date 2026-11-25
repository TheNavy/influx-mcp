FROM python:3.15-alpine

# The server needs nothing but the Python standard library.
WORKDIR /app
COPY influx_mcp.py ./

# Run without root privileges.
RUN adduser -D -H -u 10001 mcp
USER mcp

ENV PYTHONUNBUFFERED=1 \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=5001
EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import os,urllib.request as u; u.urlopen('http://127.0.0.1:%s/health' % os.environ.get('MCP_PORT','5001'), timeout=3)"

CMD ["python", "influx_mcp.py"]
