FROM debian:bullseye

# Set the non-root user and group
ARG USER_NAME=vscode
ARG GROUP_NAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000

# Create a non-root user with sudo privileges
RUN groupadd -g ${USER_GID} ${GROUP_NAME} && \
    useradd -u ${USER_UID} -g ${USER_GID} -m -s /bin/bash ${USER_NAME} && \
    mkdir -p /etc/sudoers.d && \
    echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USER_NAME} && \
    chmod 0440 /etc/sudoers.d/${USER_NAME}

# Create the missing directory and set permissions
RUN mkdir -p /var/lib/apt/lists/partial \
    && chown -R ${USER_NAME}:${GROUP_NAME} /var/lib/apt/lists

# Environment variables
ENV SPARK_HOME /home/${USER_NAME}/spark
ENV POETRY_HOME /home/${USER_NAME}/.poetry
ENV SPARK_FILE spark-3.5.0-bin-hadoop3.tgz
ENV SPARK_TAR spark-3.5.0-bin-hadoop3
ENV SPARK_URL https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
ENV SPARK_NO_DAEMONIZE true

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    bzip2 \
    xvfb \
    zip \
    unzip \
    tar \
    gzip \
    nano \
    vim \
    sudo \
    procps \
    python3 \
    python3-pip \
    python3-venv \
    git \
    sqlite3 \
    openjdk-11-jdk \
    scala \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/lib/apt/lists/partial/*

# Switch to the non-root user
USER $USER_NAME
WORKDIR ${SPARK_HOME}

# Install spark
RUN mkdir -p ${SPARK_HOME} \
    && chmod -R 755 /home/${USER_NAME} \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && curl -o ${SPARK_HOME}/${SPARK_FILE} ${SPARK_URL} \
    && tar -xf ${SPARK_HOME}/${SPARK_FILE} \
    && chown -R ${USER_NAME}:${USER_GROUP} /home/${USER_NAME}/.cache

# Add poetry to path
ENV PATH "${POETRY_HOME}/bin:$PATH"

# Add spark to path
ENV SPARK_HOME ${SPARK_HOME}/${SPARK_TAR}
ENV PATH "${SPARK_HOME}/bin:$PATH"

# Set up Spark
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
