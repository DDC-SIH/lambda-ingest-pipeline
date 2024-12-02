# Use an official Python base image
FROM public.ecr.aws/lambda/python:3.9

# Single RUN command for system packages
RUN yum -y update && \
    yum -y install curl unzip wget bash && \
    yum clean all

# Install Miniconda efficiently
RUN mkdir -p ${LAMBDA_TASK_ROOT}/miniconda3 && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ${LAMBDA_TASK_ROOT}/miniconda3/miniconda.sh && \
    bash ${LAMBDA_TASK_ROOT}/miniconda3/miniconda.sh -b -u -p ${LAMBDA_TASK_ROOT}/miniconda3 && \
    rm ${LAMBDA_TASK_ROOT}/miniconda3/miniconda.sh

ENV PATH=${LAMBDA_TASK_ROOT}/miniconda3/bin:$PATH

# Combine conda operations
RUN conda update sqlite && \
    conda install -c conda-forge libgdal=3.6 proj && \
    conda clean -a -y

# Memory-optimized GDAL settings
ENV GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
ENV CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE=YES
ENV GDAL_HTTP_TIMEOUT=3600
ENV VSI_CACHE=TRUE
ENV VSI_CACHE_SIZE=5242880  
ENV AWS_REQUEST_PAYER=requester
ENV CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif,.tiff,.h5
ENV GDAL_CACHEMAX=256
ENV VSI_MALLOC_TRIM_THRESHOLD_BYTES=5000000
ENV GDAL_MAX_DATASET_POOL_SIZE=256
ENV PROJ_LIB=${LAMBDA_TASK_ROOT}/miniconda3/share/proj

COPY . ${LAMBDA_TASK_ROOT}

# Combine pip installations
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt && \
    pip install psutil boto3

RUN chmod 755 ${LAMBDA_TASK_ROOT}/miniconda3/bin/gdal_translate

CMD ["app.lambda_handler"]