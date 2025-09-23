FROM apache/airflow:2.10.4
RUN pip install --no-cache-dir pyyaml jsonschema httpx rdflib saxonche pandas
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash && \
    export NVM_DIR="$HOME/.nvm" && \
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" && \
    nvm install 20 && \
    npm install -g @comunica/query-sparql \
