#/bin/bash

# Instalação do JDK
apt-get -y install openjdk-8-jdk-headless

# Instalação do Spark com Hadoop integrado
if [ ! -d "/opt/spark/bin" ]; then
  VER=3.5.1
    wget https://dlcdn.apache.org/spark/spark-$VER/spark-$VER-bin-hadoop3.tgz
    tar xvf spark-$VER-bin-hadoop3.tgz
    rm spark-$VER-bin-hadoop3.tgz
    mv spark-$VER-bin-hadoop3/ /opt/spark
    rm -r spark-$VER-bin-hadoop3/

    # Definindo variáveis de ambiente do Spark
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin
fi

# Instalação da biblioteca pyspark
pip install pyspark
pip install delta

# Download da base
git clone https://github.com/gsoh/VED.git
mkdir data
mv VED/Data/VED_DynamicData_Part1.7z data
mv VED/Data/VED_DynamicData_Part2.7z data
rm -r VED

# Despactação da base
apt-get install p7zip-full
cd data
7z e data/VED_DynamicData_Part1.7z
7z e data/VED_DynamicData_Part2.7z
rm data/VED_DynamicData_Part1.7z
rm data/VED_DynamicData_Part2.7z