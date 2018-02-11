

# $# is number of arguments
# in case of no arguments, exit
if [ $# -eq 0 ]
then 
echo "use like this: bash run-data.sh year st st st"
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

# shift arguments by one spot
shift

# only year, use all states
if [ $# -eq 0 ]
then 
echo "using all states"
declare -a loopstates=(al ak az ar ca co ct de dc fl ga hi id il in ia ks ky la me md ma mi mn ms mo mt ne nv nh nj nm ny nc nd oh ok or pa pr ri sc sd tn tx ut vt va wa wv wi wy)
fi

# state parameters given
if [ $# -gt 0 ]
then 
echo "using named states"
loopstates="$@"
fi


rm -rf output outputSync 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52


sudo yum install -y git
wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.2/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
nvm install node
nvm use 9

mkdir output outputSync

i=0

# loop through all states
for var in ${loopstates[@]}; do
    i=$[i + 1]
    echo "working on $var"

    # Create a separate folder and download the repo to each folder
    mkdir $i
    cd $i
    git clone https://github.com/royhobbstn/s3-db.git
    cd s3-db
    npm install
    node --max_old_space_size=4096 direct_to_s3.js $year $var &
    cd ..
    cd ..
done

wait

echo "finished.  ready to aggregate"



# al ak az ar ca co ct
# de dc fl ga hi id il
# in ia ks ky la me md
# ma mi mn ms mo mt ne
# nv nh nj nm ny nc nd
# oh ok or pa pr ri sc
# sd tn tx ut vt va wa
# wv wi wy
# us?


#calls aggregate_json
node --max_old_space_size=16384 aggregate_json.js

# sync to s3
#aws s3 sync ./outputSync s3://s3db-acs-1115