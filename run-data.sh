

# $# is number of arguments
# in case of no arguments, exit
if [ $# -eq 0 ]
then 
echo "use like this: bash run-data.sh year"
exit 1
fi

year=$1

# validate year
if [[ "$year" =~ ^(2014|2015|2016)$ ]]; then
    echo "using $year"
else
    echo "$year is not valid"
    exit 1
fi

# number of seq files in each dataset
#  '2014': '121'
#  '2015': '122'
#  '2016': '122'

num_seq=122

if [[ "$year" = 2014 ]]; then
    num_seq=121
fi


wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.2/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
nvm install node
nvm use 9
npm install

node parse-geofiles.js $year

# loop for all tracts and bgs
for i in $(seq -f "%03g" 1 "$num_seq")
do
  echo "STARTING $year $i trbg"
  node --max_old_space_size=14192 mparse.js $year $i trbg
done

# loop for all other geo
for i in $(seq -f "%03g" 1 "$num_seq")
do
  echo "STARTING $year $i allgeo"
  node --max_old_space_size=14192 mparse.js $year $i allgeo
done

echo "finished"

