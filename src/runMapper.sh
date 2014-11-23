name=$1
mkdir $1"_mapper01" $1"_mapper02" $1"_mapper03"

cd $1"_mapper01" 
java -cp .. MapperClient 8000 8080 &
cd ../$1"_mapper02"
java -cp .. MapperClient 8001 8081 &
cd ../$1"_mapper03"
java -cp .. MapperClient 8002 8082 &
