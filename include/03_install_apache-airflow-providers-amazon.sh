#!/bin/bash
PACKAGE_VERSION=4.1.0
PACKAGE_NAME=apache-airflow-providers-amazon
provider_download_dir=$(mktemp -d)
pip download --no-deps "${PACKAGE_NAME}==${PACKAGE_VERSION}" --dest "${provider_download_dir}"
curl "https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-4.1.0-py3-none-any.whl.asc" \
    -L -o "${provider_download_dir}/apache_airflow_providers_amazon-4.1.0-py3-none-any.whl.asc"
curl "https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-4.1.0-py3-none-any.whl.sha512" \
    -L -o "${provider_download_dir}/apache_airflow_providers_amazon-4.1.0-py3-none-any.whl.sha512"
echo
echo "Please verify files downloaded to ${provider_download_dir}"
ls -la "${provider_download_dir}"
echo