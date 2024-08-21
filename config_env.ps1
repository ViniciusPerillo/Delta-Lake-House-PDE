# Baixar e Instalar o JDK Manualmente
# (Para esta etapa, siga as instruções manuais ou use Chocolatey)
# Exemplo usando Chocolatey (instale Chocolatey antes se necessário):
'''
choco install openjdk8

# Instalação do Spark com Hadoop integrado
if (-Not (Test-Path "C:\opt\spark\bin")) {
    $VER = "3.5.1"
    Invoke-WebRequest -Uri "https://dlcdn.apache.org/spark/spark-$VER/spark-$VER-bin-hadoop3.tgz" -OutFile "spark-$VER-bin-hadoop3.tgz"
    tar -xvf "spark-$VER-bin-hadoop3.tgz"  # Certifique-se de ter o tar no seu PATH
    Remove-Item "spark-$VER-bin-hadoop3.tgz"
    Move-Item "spark-$VER-bin-hadoop3" -Destination "C:\opt\spark"
    Remove-Item -Recurse -Force "spark-$VER-bin-hadoop3"

    # Definindo variáveis de ambiente do Spark
    [System.Environment]::SetEnvironmentVariable('SPARK_HOME', 'C:\opt\spark', 'User')
    $env:Path += ";C:\opt\spark\bin"
}

# Instalação da biblioteca pyspark
pip install pyspark
pip install delta-spark==3.2.0
'''

# Download da base
git clone https://github.com/gsoh/VED.git
New-Item -ItemType Directory -Path "data"
Move-Item -Path "VED\Data\VED_DynamicData_Part1.7z" -Destination "data\"
Move-Item -Path "VED\Data\VED_DynamicData_Part2.7z" -Destination "data\"
Remove-Item -Recurse -Force "VED"

# Descompactação da base
# Certifique-se de ter 7-Zip instalado e no PATH ou use o caminho completo para o executável 7z.exe
Start-Process "7z" -ArgumentList "e data\VED_DynamicData_Part1.7z -o data\" -Wait
Start-Process "7z" -ArgumentList "e data\VED_DynamicData_Part2.7z -o data\" -Wait
Remove-Item "data\VED_DynamicData_Part1.7z"
Remove-Item "data\VED_DynamicData_Part2.7z"
