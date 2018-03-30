

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

# set number of seq files and bucket for each dataset
num_seq=0
bucket=""

if [[ "$year" = 2014 ]]; then
    num_seq=121
    bucket="s3db-acs-1014"
fi

if [[ "$year" = 2015 ]]; then
    num_seq=122
    bucket="s3db-acs-1115"
fi

if [[ "$year" = 2016 ]]; then
    num_seq=122
    bucket="s3db-acs-1216"
fi

wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.2/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
nvm install node
npm install


# loop for all tracts and bgs
for i in $(seq -f "%03g" 1 "$num_seq")
do
  echo "STARTING $year $i trbg"
  node --max_old_space_size=14192 mparse.js $year $i trbg
  aws s3 sync ./CensusDL/output s3://"$bucket" --content-encoding gzip --content-type application/json
done

# loop for all other geo
for i in $(seq -f "%03g" 1 "$num_seq")
do
  echo "STARTING $year $i allgeo"
  node --max_old_space_size=14192 mparse.js $year $i allgeo
  aws s3 sync ./CensusDL/output s3://"$bucket" --content-encoding gzip --content-type application/json
done

echo "finished"

