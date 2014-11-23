name=$1
mkdir $1"_reducer01" $1"_reducer02" $1"_reducer03"

cd $1"_reducer01" 
java -cp .. ReducerClient 7000 7010 7020 &
cd ../$1"_reducer02"
java -cp .. ReducerClient 7001 7011 7021 &
cd ../$1"_reducer03"
java -cp .. ReducerClient 7002 7012 7022 &
