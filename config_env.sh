#/bin/bash

# Instalação do JDK
apt-get -y install openjdk-8-jdk-headless

# Instalação do Spark com Hadoop integrado
if [ ! -d "/opt/spark/bin" ]; then
  VER=3.5.2
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
pyspark --packages com.crealytics:spark-excel_2.12:0.18.7
pip install delta-spark==3.2.0

# Download da base
git clone https://github.com/gsoh/VED.git
mkdir data/dynamic
mkdir data/static
mv VED/Data/VED_DynamicData_Part1.7z data/dynamic
mv VED/Data/VED_DynamicData_Part2.7z data/dynamic
mv VED/Data/VED_Static_Data_ICE\&HEV.xlsx data/static
mv VED/Data/VED_Static_Data_PHEV\&EV.xlsx data/static
rm -r VED

# Despactação da base
apt-get install p7zip-full
cd data/dynamic
7z e VED_DynamicData_Part1.7z
7z e VED_DynamicData_Part2.7z
rm VED_DynamicData_Part1.7z
rm VED_DynamicData_Part2.7z

cd ..
