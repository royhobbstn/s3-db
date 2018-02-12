

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

rm -rf output outputSync 1 2 3 4 5 6 7 8


wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.2/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
nvm install node
nvm use 9
npm install

mkdir output outputSync 1 2 3 4 5 6 7 8


cd 1
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year al ak az ar ca co ct &
cd ../..

cd 2
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year de dc fl ga hi id il &
cd ../..

: <<'END'

cd 3
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year in ia ks ky la me md &
cd ../..

cd 4
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year ma mi mn ms mo mt ne &
cd ../..

cd 5
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year nv nh nj nm ny nc nd &
cd ../..

cd 6
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year oh ok or pa pr ri sc &
cd ../..

cd 7
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js $year sd tn tx ut vt va wa &
cd ../..

cd 8
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
# "us" as additional state?
node --max_old_space_size=4096 direct_to_s3.js $year wv wi wy &
cd ../..

END

wait

echo "finished.  ready to aggregate"

#calls aggregate_json
node --max_old_space_size=4192 aggregate_json.js 0 &
node --max_old_space_size=4192 aggregate_json.js 1 &
node --max_old_space_size=4192 aggregate_json.js 2 &
node --max_old_space_size=4192 aggregate_json.js 3 &
node --max_old_space_size=4192 aggregate_json.js 4 &
node --max_old_space_size=4192 aggregate_json.js 5 &
node --max_old_space_size=4192 aggregate_json.js 6 &
node --max_old_space_size=4192 aggregate_json.js 7 &
node --max_old_space_size=4192 aggregate_json.js 8 &
node --max_old_space_size=4192 aggregate_json.js 9 &

wait

echo "finished aggregating"

# sync to s3
#aws s3 sync ./outputSync s3://s3db-acs-1115