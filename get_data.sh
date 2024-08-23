# Download da base
git clone https://github.com/gsoh/VED.git
mkdir data
cp VED/Data/VED_DynamicData_Part1.7z data
cp VED/Data/VED_DynamicData_Part2.7z data

# Despactação da base
sudo apt-get install p7zip-full -y
cd data
7z e VED_DynamicData_Part1.7z
7z e VED_DynamicData_Part2.7z
rm VED_DynamicData_Part1.7z
rm VED_DynamicData_Part2.7z
rm -rf VED